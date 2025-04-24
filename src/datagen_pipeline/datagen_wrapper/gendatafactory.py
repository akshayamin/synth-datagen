from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType, DateType
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_json
from datetime import datetime
from pyspark.sql.functions import udf
import logging
import sys
from pyspark.sql.functions import monotonically_increasing_id
import random
import dbldatagen as dg
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, lit, struct, explode, array, collect_set, min, max, count
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)

class GendataFactory:
    """
    Provides methods to support data generation and manipulation in Spark.    
    This utility class includes methods to generate, transform, and persist data
    catering to specific requirements like generating updates, handling schemas,
    and working with JSON structures.
    """
    def check_null_variables(variables):
        """
        Validates if the necessary variables are provided.
        
        Args:
            variables (dict): A dictionary mapping variable names to their values.
            
        Raises:
            ValueError: If any required variable is not set.
        """
        for name, var in variables.items():
            if var is None:
                raise ValueError(f"Config {name} needs to be set.")

    def check_source_not_equals_target(source, target):
        """
        Ensures the source and target are not the same.
        
        Args:
            source (str): Source table name.
            target (str): Target table name.
            
        Raises:
            ValueError: If source and target are identical.
        """
        if source == target:
            raise ValueError(f"Source cannot be same as target")

    def get_partitions(spark, table_or_mv_name):
        """
        Retrieves partition information for a given table.
        
        Args:
            spark (SparkSession): Active Spark session.
            table_or_mv_name (str): Name of the table or materialized view.
            
        Returns:
            list: A list of partition column names.
        """
        # Perform the describe table SQL command
        desc_table_df = spark.sql(f"DESCRIBE {table_or_mv_name}")

        # Add an incremental id to each row to help identify the row number
        desc_table_df = desc_table_df.withColumn("row_id", monotonically_increasing_id())

        # Find the row id of the "# Partition Information" marker
        partition_info_row = desc_table_df.filter(desc_table_df["col_name"] == "# Partition Information").head(1)
        partition_info_exists = False if len(partition_info_row) == 0 else True

        partition_columns_list = []
        # If partition_info_index is None, the table has no partition columns
        if partition_info_exists:
            partition_info_row_id = desc_table_df.filter(desc_table_df["col_name"] == "# Partition Information") \
                                                .select("row_id") \
                                                .collect()[0]["row_id"]
            # Filter out the rows below the "# Partition Information" marker
            partition_columns_df = desc_table_df.filter(desc_table_df["row_id"] > partition_info_row_id)

            # Remove the row with "# col_name" to only keep actual partition columns
            partition_columns_df = partition_columns_df.filter(partition_columns_df["col_name"] != "# col_name")
            partition_columns_list = [row.col_name for row in partition_columns_df.collect()]
        return partition_columns_list

    def build_struct(schema):
        """
        Recursively constructs a PySpark StructType based on the provided schema definition.
        
        Args:
            schema (dict or list): Schema definition.
            
        Returns:
            StructType: The constructed StructType.
        """
        if isinstance(schema, list):
            fields = []
            for item in schema:
                if isinstance(item, dict):
                    for key, value in item.items():
                        fields.append(struct(*GendataFactory.build_struct(value)).alias(key))
                else:
                    fields.append(col(item))
            return fields
        elif isinstance(schema, dict):
            return [struct(*GendataFactory.build_struct(value)).alias(key) for key, value in schema.items()]
        else:
            return [col(schema)]

    def gen_json_struct(input_df, json_target_mapper_str, additional_root_fields_to_extract):
        """
        Generates a structured JSON DataFrame from the input based on mapping and additional fields.
        
        Args:
            input_df (DataFrame): Input DataFrame to be transformed.
            json_target_mapper_str (dict): Mapping definition for the JSON structure.
            additional_root_fields_to_extract (list): Additional fields to include at the root level.
            
        Returns:
            DataFrame: Transformed DataFrame with structured JSON.
        """
        column_expressions = {}
        for root_key, value in json_target_mapper_str.items():
            column_expressions[root_key] = struct(*GendataFactory.build_struct(value)).alias(root_key)

        # Selecting and structuring the columns as per the dynamic schema
        select_expr = [to_json(column_expressions[key]).alias(key) for key in column_expressions]
        
        try:
            output_df = input_df.select(*(additional_root_fields_to_extract or []), *select_expr)
        except Exception as e:
            logging.error(f"Reading json fields failed with error: {e}")
            sys.exit(1)
        return output_df

    def num_hive_partitions_check(spark, gen_df, partitions_list, partitionoverride=False):
        """
        Validates the number of generated Hive partitions.
        
        Args:
            spark (SparkSession): Active Spark session.
            gen_df (DataFrame): Generated DataFrame to check.
            partitions_list (list): List of partition columns.
            partitionoverride (bool): Whether to override partition limit restrictions.
            
        Raises:
            Warning or Error based on partition counts and configuration.
        """
        if partitions_list:
            num_partitions = gen_df.select(partitions_list).distinct().count()
            total_hive_partitions = num_partitions
            logging.info(f"total hive partitions created for columns {str(partitions_list)}: {total_hive_partitions}")
            if(not partitionoverride):
                if(1000 <= total_hive_partitions < 5000):
                    logging.warning("Total number of partitions is between 1000 and 5000")
                elif total_hive_partitions >= 5000:
                    logging.error("Total number of partitions is greater than or equal to 5000. This is an error. Please reduce the number of partitions or override if really needed. set variable partitionoverride=True in initial configs")
                    sys.exit(1)
        else:
            logging.info("No hive partition columns specified")

    # Generic function to pivot DataFrame without mentioning column names explicitly
    def pivot_summary(df, columns):
        """
        Pivots a summary DataFrame based on provided columns.
        
        Args:
            df (DataFrame): DataFrame containing summary information.
            columns (list): Columns to pivot on.
            
        Returns:
            DataFrame: Pivoted DataFrame.
        """
        output_df = None
        for column in columns:
            temp_df = df.select(
                F.lit(column).alias("Field"), 
                F.collect_list(df["summary"]).alias("Key"),
                F.collect_list(df[column].cast("float")).alias("Values")
            )

            if output_df is None:
                output_df = temp_df
            else:
                output_df = output_df.union(temp_df)
        return output_df

    def pivot_df(df, categorical_fields, values_field_name):
        """
        Performs a pivot transformation on a DataFrame based on the specified categorical fields.

        Args:
            df (DataFrame): The DataFrame to be pivoted.
            categorical_fields (list): The list of fields based on which to perform the pivot.

        Returns:
            DataFrame: The resulting DataFrame after the pivot operation.
        """
        exprs = [f"'{col_name}', {col_name}" for col_name in categorical_fields]
        expr_str = ", ".join(exprs)
        pivoted_df = df.selectExpr(f"stack({len(categorical_fields)}, {expr_str}) as (Field, {values_field_name})")
        return pivoted_df

    def eval_initial_colspecs(spark, initial_colspecs, derived_reference_tbl, compute_stats_table, source_reference_tbl):
        """
        Evaluates and possibly updates initial column specifications based on derived statistics from a reference table.

        Args:
            spark (SparkSession): The current Spark session.
            initial_colspecs (dict): Initial specifications for data generation.
            derived_reference_tbl (str): Reference table for deriving stats.
            compute_stats_table (str): Table where computed stats are stored.
            source_reference_tbl (str): Source table used as a reference for schema and stats.

        Returns:
            dict: Updated column specifications.
        """

        categorical_fields = [specs["field"] for key, specs in initial_colspecs.items() if specs.get("derive") and specs.get("type") == "categorical"]
        continuous_fields = [specs["field"] for key, specs in initial_colspecs.items() if specs.get("derive") and specs.get("type") == "continuous"]

        if categorical_fields or continuous_fields:
            # Proceed only if at least one of the field lists is non-empty

            # Check stats_tbl is not None before proceeding            
            stats_tbl = compute_stats_table
            if stats_tbl is None:
                raise ValueError("compute_stats_table must not be None when derived fields present in colspec_overrides.")
            GendataFactory.createstatstableifnotexists(spark, compute_stats_table)
            derived_values_df = spark.sql(f"select * from {stats_tbl} where tablename = '{source_reference_tbl}'")

            # Determine which fields need to be computed or deleted based on their presence in the stats table
            existing_fields_df = derived_values_df.select("Field").distinct()
            existing_fields = [row['Field'] for row in existing_fields_df.collect()]

            # Fields to be computed: present in initial_colspecs but not in the stats table
            fields_to_compute_cat = set(categorical_fields) - set(existing_fields)
            fields_to_compute_cont = set(continuous_fields) - set(existing_fields)

            # Fields to be deleted: present in the stats table but not in initial_colspecs
            fields_to_delete = set(existing_fields) - (set(categorical_fields) | set(continuous_fields))

            # Delete entries for fields that are no longer needed
            if fields_to_delete:
                for field in fields_to_delete:
                    spark.sql(f"DELETE FROM {stats_tbl} WHERE Field = '{field}' AND TableName = '{source_reference_tbl}'")

            # Generate values only for fields that need to be computed
            if fields_to_compute_cat or fields_to_compute_cont:
                derived_values_df = GendataFactory.generate_derived_values(spark, derived_reference_tbl, fields_to_compute_cat, fields_to_compute_cont, source_reference_tbl, stats_tbl)

            # Update initial_colspecs with the latest values
            updated_derived_values_df = spark.sql(f"select * from {stats_tbl} where tablename = '{source_reference_tbl}'")
            initial_colspecs = GendataFactory.eval_variable_specs(updated_derived_values_df, initial_colspecs)

        return initial_colspecs


    def generate_derived_values(spark, derived_reference_tbl, fields_to_compute_cat, fields_to_compute_cont, source_reference_tbl, stats_tbl):
        """
        Generates derived values based on the categorical and continuous fields specified, storing the results in a statistics table.

        Args:
            spark (SparkSession): The current Spark session.
            derived_reference_tbl (str): The table from which to derive values.
            fields_to_compute_cat (set): Fields for which to compute categorical values.
            fields_to_compute_cont (set): Fields for which to compute continuous values.
            source_reference_tbl (str): Source table to associate the derived values.
            stats_tbl (str): The table where the derived statistics are stored.

        Returns:
            DataFrame: A DataFrame containing the derived statistics.
        """
        # spark.sql(f"delete from {stats_tbl} where tablename = '{source_reference_tbl}'")
        stats_schema = spark.read.table(stats_tbl).schema
        derived_reference_df = spark.sql(f"select * from {derived_reference_tbl}")
        # Create aggregations for categorical and continuous fields
        categorical_fields = fields_to_compute_cat
        continuous_fields = fields_to_compute_cont

        collect_set_columns = [collect_set(col(name)).alias(name) for name in categorical_fields]
        continuous_aggregations = []
        for field in continuous_fields:
            continuous_aggregations.append(struct(
                min(col(field)).cast("double").alias("min"),
                max(col(field)).cast("double").alias("max"),
                count(col(field)).alias("ct")
            ).alias(field))

        # Combine all aggregations
        all_aggregations = collect_set_columns + continuous_aggregations

        # Perform aggregation
        result_df = derived_reference_df.select(*all_aggregations)
        
        categorical_df = spark.createDataFrame([], stats_schema)
        if len(categorical_fields) != 0:
            # Now use the function
            pivoted_df = GendataFactory.pivot_df(result_df, categorical_fields, "CategoricalValues") \
                .withColumn("Type", lit("categorical")) \
                .withColumn("TableName", lit(source_reference_tbl)) \
                .withColumn("ContinuousValues",lit(None)) \
                .withColumn("InsertTs", F.current_timestamp())
            orderedColumns = ["TableName", "Field", "Type", "CategoricalValues", "ContinuousValues", "InsertTs"]
            categorical_df = pivoted_df.select(*orderedColumns)

        continuous_df = spark.createDataFrame([], stats_schema)
        if len(continuous_fields) != 0:
            # Assuming 'result_df' already has the continuous fields structured with their statistics
            # Create an array of structs combining field name with its statistics struct for continuous fields
            continuous_fields_exprs = [
                struct(F.lit(field).alias("Field"), col(field).alias("ContinuousValues")) for field in continuous_fields
            ]

            # Flatten the continuous fields structure into a two-column format
            con_df = result_df.select(explode(array(*continuous_fields_exprs)).alias("data")).select(
                "data.Field",
                "data.ContinuousValues"
            ).withColumn("Type", lit("continuous")) \
            .withColumn("TableName", lit(source_reference_tbl)) \
            .withColumn("CategoricalValues",lit(None)) \
            .withColumn("InsertTs", F.current_timestamp())
            orderedColumns = ["TableName", "Field", "Type", "CategoricalValues", "ContinuousValues", "InsertTs"]
            continuous_df = con_df.select(*orderedColumns)

        derived_values_df = categorical_df.unionAll(continuous_df)
        if(source_reference_tbl):
            derived_values_df.write.format("delta").mode("append").saveAsTable(stats_tbl)
        return derived_values_df
    
    def eval_variable_specs(derived_stats_df, initial_colspecs):
        """
        Evaluates and updates the variable specifications based on the derived statistics.
        It updates categorical variables with possible values and continuous variables with range specifications.

        Args:
            derived_stats_df (DataFrame): DataFrame containing derived statistics for each field.
            initial_colspecs (dict): Dictionary containing initial column specifications.

        Returns:
            dict: Updated column specifications with derived information integrated.
        """
        # Convert result_df to a dictionary with 'Field' as keys and 'Values' as values
        categorical_list = derived_stats_df.filter(col("type")=='categorical').select('Field', 'CategoricalValues').collect()
        categorical_dict = {row['Field']: row['CategoricalValues'] for row in categorical_list}
        # Update initial_colspecs with values from result_dict
        for key, specs in initial_colspecs.items():
            if specs.get("derive") and specs.get("type") == "categorical":
                field_name = specs["field"]
                if field_name in categorical_dict:
                    # Update initial_colspecs entry with values from categorical_dict
                    initial_colspecs[key]["values"] = categorical_dict[field_name]
                    initial_colspecs[key]["random"] = True
                    # Remove keys as they are no longer needed
                    del initial_colspecs[key]["derive"]
                    del initial_colspecs[key]["type"]
                    del initial_colspecs[key]["field"]

        # Assuming continuous_df already has "Field" and "ContinuousValues" columns
        # where "ContinuousValues" is a struct with "min", "max", and "ct" attributes
        continuous_list = derived_stats_df.filter(col("type")=='continuous').select('Field', 'ContinuousValues').collect()

        # Create a dictionary mapping field names to their statistics structs
        continuous_dict = {row['Field']: row['ContinuousValues'] for row in continuous_list}

        # Update initial_colspecs with values from continuous_dict
        for key, specs in initial_colspecs.items():
            if specs.get("derive") and specs.get("type") == "continuous":
                field_name = specs["field"]
                if field_name in continuous_dict:
                    stats = continuous_dict[field_name]
                    # Accessing min, max, and count from the struct
                    min_value = float(stats.min)
                    max_value = float(stats.max)
                    nrows = int(stats.ct)
                    # Determine the step size
                    step = (max_value - min_value) / nrows if nrows else None
                    if step and step > 1:
                        step = round(step)
                    
                    # Update initial_colspecs entry with summary statistics
                    initial_colspecs[key]["minValue"] = min_value
                    initial_colspecs[key]["maxValue"] = max_value
                    initial_colspecs[key]["step"] = step
                    initial_colspecs[key]["random"] = True
                    initial_colspecs[key]["distribution"] = "normal"
                    
                    # Remove keys as they are no longer needed
                    del initial_colspecs[key]["derive"]
                    del initial_colspecs[key]["type"]
                    del initial_colspecs[key]["field"]
        return initial_colspecs

    def generate_random_value_for_field(field_name, start_value, end_value, data_type):
        """
        Generates an expression for creating a random value for a specific field within a given range.

        Args:
            field_name (str): Name of the field for which the random value is generated.
            start_value (int/float/datetime/str): Starting value of the range.
            end_value (int/float/datetime/str): Ending value of the range.
            data_type (DataType): The data type of the field.

        Returns:
            Column: Spark SQL Column expression for generating random values.
        """
        if data_type == TimestampType():
            start_timestamp = datetime.timestamp(datetime.strptime(start_value, '%Y-%m-%d %H:%M:%S'))
            end_timestamp = datetime.timestamp(datetime.strptime(end_value, '%Y-%m-%d %H:%M:%S'))
            # Generate a random timestamp by scaling and adding it to the start timestamp
            return expr(f"CAST({start_timestamp} + ({end_timestamp} - {start_timestamp}) * rand() AS TIMESTAMP)")
        elif data_type in [IntegerType(), LongType()]:
            # Generate a random integer within the range
            return expr(f"FLOOR({start_value} + ({end_value} - {start_value}) * rand())").cast(data_type)
        elif data_type == DoubleType():
            # Generate a random double within the range
            return expr(f"{start_value} + ({end_value} - {start_value}) * rand()").cast(DoubleType())
        elif data_type == DateType():
            # Assuming start_value and end_value are date strings in the format 'YYYY-MM-DD'
            return expr(f"date_add(cast('{start_value}' as date), cast(rand() * (datediff(cast('{end_value}' as date), cast('{start_value}' as date))) as int))")
        else:
            raise ValueError(f"Unsupported data type for sequence field: {data_type}")

    def check_updatefields_not_in_sequencefields(updatefields, sequencefields):
        """
        Ensures that the fields designated for updates are not also used as sequence fields, preventing conflicts.

        Args:
            updatefields (list): List of fields to be updated.
            sequencefields (list): List of fields used as sequence identifiers.

        Raises:
            Exception: If any field is found in both update and sequence field lists.
        """
        # Check to ensure updatefields are not in sequencefields
        for field in updatefields:
            if field in sequencefields:
                logging.error(f"The field '{field}' cannot be present in both updatefields and sequencefields")
                sys.exit(1)

    def primary_key_gen(primary_key, colspec_overrides, custom_schema, dspec_delta):
        """
        Adjusts the data generation specification to ensure primary key uniqueness by modifying the specifications
        for primary key columns not already specified in colspec_overrides.

        Args:
            primary_key (list): List of primary key column names.
            colspec_overrides (dict): Column specifications that are already provided and should not be altered.
            custom_schema (StructType): Schema of the data being generated.
            dspec_delta (DataGenerator): The data generator instance being configured.

        Returns:
            tuple: A tuple containing the updated schema and DataGenerator instance.
        """
        for key in primary_key:
            if(key is not None and key not in colspec_overrides):                
                field_name=None
                data_type=None
                for field in custom_schema.fields:
                    if field.name == key:
                        field_name = field.name
                        data_type = field.dataType
                if(field_name is None):
                    logging.error(f"Primary key {key} is not present in schema")
                    sys.exit(1)
                if isinstance(data_type, StringType):
                    dspec_delta = (dspec_delta.withColumnSpec(key, format="0x%013x",baseColumn="id"))
                else:
                    dspec_delta = (dspec_delta.withColumnSpec(key, baseColumn="id"))
                primarykey_index = None
                for index, field in enumerate(custom_schema.fields):
                    if field.name == key:
                        primarykey_index = index
                        break
                new_fields = custom_schema.fields[:primarykey_index] + custom_schema.fields[primarykey_index+1:]
                custom_schema = StructType(new_fields)
        return (custom_schema, dspec_delta)
    
    def infer_columns(custom_schema, colspec_overrides, dspec_delta, default_colspecs):
        """
        Applies default column specifications and any overrides to the DataGenerator specifications,
        based on the provided schema.

        Args:
            custom_schema (StructType): Schema of the data being generated.
            colspec_overrides (dict): Column specifications provided as overrides.
            dspec_delta (DataGenerator): The data generator instance being configured.
            default_colspecs (dict): Default column specifications based on data types.

        Returns:
            DataGenerator: The data generator instance with updated specifications.
        """
        # Fields in 'initial_custom_schema'
        custom_schema_fields = set(field.name for field in custom_schema.fields)

        # Divide 'initial_colspecs' into two dictionaries
        cols_in_custom_schema = {}
        cols_not_in_custom_schema = {}

        for field, spec in colspec_overrides.items():
            if field in custom_schema_fields:
                cols_in_custom_schema[field] = spec
            else:
                cols_not_in_custom_schema[field] = spec
        for col_name, col_specs in cols_in_custom_schema.items():
            dspec_delta = dspec_delta.withColumnSpec(col_name, **col_specs)
        cols_in_custom_schema_fields = cols_in_custom_schema.keys()
        
        # Iterate through the fields
        for field in custom_schema.fields:
            field_name = field.name
            data_type = field.dataType
            if(field_name not in cols_in_custom_schema_fields):
                data_type_key = type(data_type).__name__
                if data_type_key in default_colspecs:
                    dspec_delta = dspec_delta.withColumnSpec(field_name, **default_colspecs[data_type_key])
                else:
                    pass
        for col_name, col_specs in cols_not_in_custom_schema.items():
            dspec_delta = dspec_delta.withColumn(col_name, **col_specs)
        print(dspec_delta) ## DEBUG        
        return dspec_delta.build()

    def write_delta_format(spark, df_delta, partition_col_list, spark_partitions_limit_override_flag, write_mode, target_data_path, target_table_name, run_mode):
        """
        Writes the generated data to a Delta table, with optional partitioning and schema evolution.

        Args:
            spark (SparkSession): The current Spark session.
            df_delta (DataFrame): The DataFrame to be written.
            partition_col_list (list): List of column names to partition the data by.
            spark_partitions_limit_override_flag (bool): Flag to override partition checks.
            write_mode (str): Mode for writing the data ('overwrite' or 'append').
            target_data_path (str): Path to write the data to, if specifying an external table.
            target_table_name (str): Name of the target Delta table.
            run_mode (str): Execution mode, if 'debug', the write operation is skipped.

        Returns:
            DataFrame: The DataFrame that was written, useful for chaining operations.
        """
        GendataFactory.num_hive_partitions_check(spark, df_delta, partition_col_list, spark_partitions_limit_override_flag)
        schema_evolution_type = "overwriteSchema" if(write_mode == "overwrite") else "mergeSchema"

        if(run_mode != "debug"):
            if(partition_col_list):
                df_delta = df_delta.repartition(*partition_col_list)
                if(target_data_path is not None):
                    df_delta.write.partitionBy(partition_col_list).format("delta").option(schema_evolution_type, "true").option("path",target_data_path).mode(write_mode).saveAsTable(target_table_name)
                else:
                    df_delta.write.partitionBy(partition_col_list).format("delta").option(schema_evolution_type, "true").mode(write_mode).saveAsTable(target_table_name)
            else:
                if(target_data_path is not None):
                    df_delta.write.format("delta").option(schema_evolution_type, "true").option("path",target_data_path).mode(write_mode).saveAsTable(target_table_name)
                else:
                    df_delta.write.format("delta").option(schema_evolution_type, "true").mode(write_mode).saveAsTable(target_table_name)
        return df_delta
    
    def inc_primary_key_gen(df1_inserts, primary_key, colspec_overrides, custom_schema, inc_mode_max):
        """
        Adjusts the primary key column values in the DataFrame for incremental data generation, ensuring uniqueness.

        Args:
            df1_inserts (DataFrame): The DataFrame containing the data to be inserted.
            primary_key (list): A list of primary key column names.
            colspec_overrides (dict): Column specifications that may override the default behavior.
            custom_schema (StructType): The schema of the DataFrame.
            inc_mode_max (dict): A dictionary holding the maximum current values for each primary key.

        Returns:
            DataFrame: The updated DataFrame with adjusted primary key values.
        """
        for key in primary_key:
            if(key is not None and key not in colspec_overrides):
                for field in custom_schema.fields:
                    if field.name == key:
                        fname = field.name
                        dtype = field.dataType
                if isinstance(dtype, StringType):
                    df1_inserts = df1_inserts.withColumn(key, F.expr(f"CONCAT('0x', lpad(format_string('%x', CAST(SUBSTRING({key}, 3) AS BIGINT) + int({inc_mode_max[key]})), 13, '0'))"))
                else:
                    df1_inserts = df1_inserts.withColumn(key, F.expr(f"{key} + {inc_mode_max[key]}"))

    def get_schema(spark, custom_source_schema, source_reference_df, source_reference_tbl):
        """
        Determines the schema to be used based on the provided parameters. Prioritizes direct schema input, 
        then DataFrame schema, then schema from a reference table.

        Args:
            spark (SparkSession): The active Spark session.
            custom_source_schema (StructType, optional): A directly provided schema.
            source_reference_df (DataFrame, optional): A DataFrame from which to infer the schema.
            source_reference_tbl (str, optional): A table name from which to infer the schema.

        Returns:
            StructType: The determined schema.
        """
        custom_schema = custom_source_schema if custom_source_schema is not None else (source_reference_df.schema if source_reference_df is not None else spark.read.table(source_reference_tbl).schema)
        return custom_schema
    
    def get_reference_data(spark, source_reference_df, source_reference_tbl):
        """
        Retrieves reference data based on the provided DataFrame or table name, to be used for data derivation.

        Args:
            spark (SparkSession): The active Spark session.
            source_reference_df (DataFrame, optional): The DataFrame providing reference data.
            source_reference_tbl (str, optional): The table name providing reference data.

        Returns:
            str: The name of the temporary view created from the reference data.
        """
        if(source_reference_df is not None):
            source_reference_df.createOrReplaceTempView("reference_tbl")
            derived_reference_tbl = "reference_tbl"
        elif(source_reference_tbl is not None):
            spark.sql(f"select * from {source_reference_tbl}").createOrReplaceTempView("reference_tbl")
            derived_reference_tbl = "reference_tbl"
        else:
            derived_reference_tbl=None
        return derived_reference_tbl

    def get_partition_list(spark, partition_col_list, source_reference_tbl, custom_schema):
        """
        Determines the list of partition columns for the data, either from the provided list or inferred from the reference table.

        Args:
            spark (SparkSession): The active Spark session.
            partition_col_list (list, optional): Explicitly provided list of partition columns.
            source_reference_tbl (str, optional): A table name from which to infer partition columns.
            custom_schema (StructType): The schema of the data.

        Returns:
            list: A list of partition column names.
        """
        partition_list = partition_col_list if partition_col_list is not None else (GendataFactory.get_partitions(spark, source_reference_tbl) if source_reference_tbl is not None else [])
        if(partition_col_list):
            for key in partition_col_list:
                field_name=None
                for field in custom_schema.fields:
                    if field.name == key:
                        field_name = field.name
                if(field_name is None):
                    logging.error(f"Partition column {key} is not present in schema")
                    sys.exit(1)
        return partition_list
    
    def get_source_data(spark, json_source_df, json_source_table_name):
        """
        Retrieves source data to be processed, either directly from the provided DataFrame or from the specified table.

        Args:
            spark (SparkSession): The active Spark session.
            json_source_df (DataFrame, optional): Directly provided source data as a DataFrame.
            json_source_table_name (str, optional): A table name from which to retrieve source data.

        Returns:
            DataFrame: The source data.
        """
        source_df = json_source_df if json_source_df is not None else spark.read.table(json_source_table_name)
        return source_df

    def write_json(json_df, jsondata_file_format, json_target_write_method, jsondata_compression, json_target_path, run_mode):
        """
        Writes the provided DataFrame in JSON format to the specified path, with options for compression and write mode.

        Args:
            json_df (DataFrame): The DataFrame to be written out.
            jsondata_file_format (str): The format in which to save the data (e.g., 'json').
            json_target_write_method (str): Write method (e.g., 'append', 'overwrite').
            jsondata_compression (str): Compression method (e.g., 'gzip').
            json_target_path (str): The target path for the output files.
            run_mode (str): The run mode, which if set to 'debug', will skip the actual write.

        Returns:
            DataFrame: The DataFrame that was intended to be written (useful for chaining operations).
        """
        if(run_mode != "debug"):
            if(jsondata_compression is not None):
                json_df.write.format(jsondata_file_format).mode(json_target_write_method).option("compression", jsondata_compression).save(json_target_path)
            else:
                json_df.write.format(jsondata_file_format).mode(json_target_write_method).save(json_target_path)
        return json_df

    def make_pk_unique(spark, df1_inserts, source_data_df, primary_key, colspec_overrides, custom_schema, insert_filter, inc_mode_max):
        """
        Ensures the uniqueness of primary key values in the insert DataFrame by incrementing existing values based on the source data.

        Args:
            spark (SparkSession): The active Spark session.
            df1_inserts (DataFrame): The DataFrame with insert data.
            source_data_df (DataFrame): The DataFrame representing the existing data to compare against.
            primary_key (list): The list of primary key column names.
            colspec_overrides (dict): Column specifications that may override default behaviors.
            custom_schema (StructType): The schema of the DataFrame.

        Returns:
            DataFrame: The DataFrame with updated primary key values ensuring uniqueness.
        """
        # inc_mode_max = {}
        # for key in primary_key:
        #     inc_mode_max[key] = source_data_df.filter(insert_filter).select(F.max(f'{key}')).collect()[0][0]

        cal_inc_mode_max={}
        if(inc_mode_max is None):
            # Apply the insert_filter to reduce the dataset if necessary.
            filtered_df = source_data_df.select(*primary_key).filter(insert_filter)
            if filtered_df.head(1) == []:
                raise ValueError("The filtered DataFrame is empty. Cannot determine maximum values for primary keys.")
            # Use the agg function to calculate the max for all primary key columns in one go.
            # The dictionary comprehension builds an aggregation dictionary to find the max of each key.
            aggregation_exprs = [F.max(key).alias(key) for key in primary_key]
            # Apply the aggregation
            max_values = filtered_df.agg(*aggregation_exprs).collect()[0].asDict()
            # Remap the max_values dictionary keys to remove the 'max(' prefix and ')' suffix.
            cal_inc_mode_max = max_values
        else:
            cal_inc_mode_max=inc_mode_max

        # Increment hex UDF
        def increment_hex(hex_str, increment):
            # Convert hex to integer, increment, and back to hex
            int_val = int(hex_str[2:], 16)  # Convert hex to int without '0x'
            incremented_val = int_val + int(increment, 16) + 1  # Add integer increment
            return '0x' + format(incremented_val, 'x').zfill(13)  # Back to hex, padded

        increment_hex_udf = udf(increment_hex, StringType())

        for key in primary_key:
            if(key is not None and key not in colspec_overrides):
                for field in custom_schema.fields:
                    if field.name == key:
                        fname = field.name
                        dtype = field.dataType
                if isinstance(dtype, StringType):
                    df1_inserts = df1_inserts.withColumn(key, increment_hex_udf(key, F.lit(cal_inc_mode_max[key])))
                else:
                    df1_inserts = df1_inserts.withColumn(key, F.expr(f"{key} + {cal_inc_mode_max[key]+1}"))

        # Apply the insert_filter to reduce the dataset if necessary.
        df1_inserts_pk = df1_inserts.select(*primary_key)
        if df1_inserts_pk.head(1) == []:
            raise ValueError("The inserts DataFrame is empty. Cannot determine maximum values for primary keys.")
        aggregation_exprs = [F.max(key).alias(key) for key in primary_key]
        # Apply the aggregation
        max_values = df1_inserts_pk.agg(*aggregation_exprs).collect()[0].asDict()
        next_run_inc_mode_max = max_values
        return (df1_inserts, next_run_inc_mode_max)

    def generate_updates(spark, update_filter, update_records_count, source_data_df, updatefields, update_field_stats_table, target_table_name, update_values_limit):
        """
        Generates a DataFrame of updates by applying a given update filter to the source data and modifying specified fields.

        Args:
            update_filter (Column): A filter condition used to select rows that will be updated.
            update_records_count (int): The maximum number of records to update.
            source_data_df (DataFrame): The DataFrame containing the original data.
            updatefields (list): List of field names that should be updated.

        Returns:
            DataFrame: A DataFrame containing the updated records.
        """
        if(updatefields):
            if update_field_stats_table is None:
                raise ValueError("compute_stats_table must not be None when derived fields present in colspec_overrides.")
            GendataFactory.createupdatefieldstatstableifnotexists(spark, update_field_stats_table)
            derived_values_df = spark.sql(f"select * from {update_field_stats_table} where tablename = '{target_table_name}'")

            # Determine which fields need to be computed or deleted based on their presence in the stats table
            existing_fields_df = derived_values_df.select("Field").distinct()
            existing_fields = [row['Field'] for row in existing_fields_df.collect()]

            # Fields to be computed: present in initial_colspecs but not in the stats table
            fields_to_compute = set(updatefields) - set(existing_fields)

            # Fields to be deleted: present in the stats table but not in initial_colspecs
            fields_to_delete = set(existing_fields) - set(updatefields)

            # Delete entries for fields that are no longer needed
            if fields_to_delete:
                for field in fields_to_delete:
                    spark.sql(f"DELETE FROM {update_field_stats_table} WHERE Field = '{field}' AND TableName = '{target_table_name}'")
            

            # Generate values only for fields that need to be computed
            if fields_to_compute:
                derived_values_df = GendataFactory.generate_update_values(spark,
                                                                          source_data_df, 
                                                                          fields_to_compute, 
                                                                          update_field_stats_table,
                                                                          target_table_name,
                                                                          update_values_limit)

            # Update initial_colspecs with the latest values
            updated_derived_values_df = spark.sql(f"select * from {update_field_stats_table} where tablename = '{target_table_name}'")
            df1_updates = GendataFactory.pick_update_value(updated_derived_values_df, source_data_df, update_filter, update_records_count)
            update_schema = source_data_df.select(*updatefields)

            schema_dict = {field.name: field.dataType.simpleString() for field in update_schema.schema.fields}
            # Now construct the select expressions using the schema dictionary for data type
            select_exprs = [
                f"CAST({col_name} AS {schema_dict[col_name]}) AS {col_name}"
                if col_name in updatefields else col_name
                for col_name in df1_updates.columns
            ]
            # Apply the selectExpr with the constructed expressions to cast the specified columns
            df1_updates_final = df1_updates.selectExpr(select_exprs)
        return df1_updates_final
    
    def generate_update_values(spark,
                                source_data_df, 
                                fields_to_compute, 
                                update_field_stats_table,
                                target_table_name,
                                update_values_limit):

        update_stats_schema = spark.read.table(update_field_stats_table).schema


        # Placeholder for null values
        null_placeholder = "__NULL__VALUE__"

        # Iterate over fields_to_compute to cast each field to a string type
        casted_source_data_df = source_data_df.select(*fields_to_compute)
        for field in fields_to_compute:
            casted_source_data_df = casted_source_data_df.withColumn(field, col(field).cast("string"))

        # Replace nulls in DataFrame with placeholder
        nafilled_source_data_df = casted_source_data_df.na.fill(null_placeholder)
        collect_set_columns = [collect_set(col(name)).alias(name) for name in fields_to_compute]
        # Combine all aggregations
        all_aggregations = collect_set_columns
        # Perform aggregation
        result_df = nafilled_source_data_df.select(*all_aggregations)
        # Apply the slicing to limit each aggregated set to up to 100 items
        for field in fields_to_compute:
            result_df = result_df.withColumn(
                field, 
                F.expr(f"slice({field}, 1, {update_values_limit})")
            )

        # Iterate over each field and apply the transformation
        for field_name in fields_to_compute:
            result_df = result_df.withColumn(
                field_name,
                expr(f"TRANSFORM({field_name}, x -> IF(x = '__NULL__VALUE__', NULL, x))")
            ) 

        update_values_df = spark.createDataFrame([], update_stats_schema)

        if len(fields_to_compute) != 0:
            # Now use the function
            pivoted_df = GendataFactory.pivot_df(result_df, fields_to_compute, "UpdateValues") \
                .withColumn("TableName", lit(target_table_name)) \
                .withColumn("InsertTs", F.current_timestamp())
            orderedColumns = ["TableName", "Field", "UpdateValues", "InsertTs"]
            update_values_df = pivoted_df.select(*orderedColumns)
        if(target_table_name):
            update_values_df.write.format("delta").mode("append").saveAsTable(update_field_stats_table)
        return update_values_df

    def pick_update_value(updated_derived_values_df, source_data_df, update_filter, update_records_count):
        # Convert result_df to a dictionary with 'Field' as keys and 'Values' as values
        update_list = updated_derived_values_df.select('Field', 'UpdateValues').collect()
        update_dict = {row['Field']: row['UpdateValues'] for row in update_list}
        # Apply the filter condition
        df_filtered = source_data_df.filter(update_filter).limit(update_records_count)

        # Function to pick a different value from a list
        def pick_different_value(current_value, value_list):
            value_set = set(value_list)
            value_set.discard(current_value)  # Remove the current value if it exists
            if value_set:
                return random.choice(list(value_set))
            else:
                return current_value  # Return the current value if no different value is available

        # Register the function as a UDF
        pick_different_value_udf = udf(pick_different_value, StringType())  # Adjust the return type based on your field
        
        df1_updates = df_filtered
        # Loop through the fields specified in updatefields
        for field, updatevalues in update_dict.items():
            if(len(updatevalues)==1):
                logging.warn(f"Field {field} has only 1 distinct value {str(updatevalues)}. No values will be updated")
            else:
                df1_updates = df1_updates.withColumn(field, pick_different_value_udf(col(field), F.lit(updatevalues)))
        return df1_updates
            
    def generate_sequence_fields(df1_inserts, df1_updates, sequencefields, custom_schema):
        """
        Adds sequence fields to both insert and update DataFrames, generating values within specified ranges.

        Args:
            df1_inserts (DataFrame): The DataFrame containing new records to insert.
            df1_updates (DataFrame): The DataFrame containing existing records to update.
            sequencefields (dict): A dictionary specifying the sequence field names and their value ranges.
            custom_schema (StructType): The schema of the DataFrame.

        Returns:
            DataFrame: A combined DataFrame with sequence fields added to both inserts and updates.
        """

        # Generate the DataFrames
        for field, ranges in sequencefields.items():
            data_type = next((f.dataType for f in custom_schema.fields if f.name == field), None)
            if data_type:
                df1_inserts = df1_inserts.withColumn(field, GendataFactory.generate_random_value_for_field(field, ranges["insertstart"], ranges["insertend"], data_type))
                df1_updates = df1_updates.withColumn(field, GendataFactory.generate_random_value_for_field(field, ranges["updatestart"], ranges["updateend"], data_type))        
        df_changes = df1_inserts.union(df1_updates)
        return df_changes

    def generate_inserts(spark, 
                         source_reference_tbl, 
                         source_reference_df, 
                         target_table_name, 
                         custom_source_schema, 
                         partition_col_list, 
                         colspec_overrides, 
                         nrows, 
                         n_gen_partitions, 
                         primary_key, 
                         default_colspecs,
                         compute_stats_table):
        """
        Generates a DataFrame of new records to be inserted, using either a provided schema or referencing an existing table.

        Args:
            spark (SparkSession): The current Spark session.
            source_reference_tbl (str): A reference table to derive the schema, if no custom schema is provided.
            source_reference_df (DataFrame): A reference DataFrame to derive the schema, if provided.
            target_table_name (str): The name of the target table for data insertion.
            custom_source_schema (StructType): A custom schema for data generation, if provided.
            partition_col_list (list): List of columns to partition the data by.
            colspec_overrides (dict): Specific column generation rules that override the defaults.
            nrows (int): The number of rows to generate.
            n_gen_partitions (int): The number of partitions to use for data generation.
            primary_key (list): List of primary key columns to ensure uniqueness.
            default_colspecs (dict): Default column generation rules.
            compute_stats_table (str): The table where computed statistics are stored.

        Returns:
            DataFrame: The generated DataFrame of new records to insert.
        """
        GendataFactory.check_source_not_equals_target(source_reference_tbl, target_table_name)
        custom_schema = GendataFactory.get_schema(spark, custom_source_schema, source_reference_df, source_reference_tbl)
        GendataFactory.check_colspec_overrides_additional_fields(custom_schema, colspec_overrides)
        derived_reference_tbl = GendataFactory.get_reference_data(spark, source_reference_df, source_reference_tbl)
        partition_col_list = GendataFactory.get_partition_list(spark, partition_col_list, source_reference_tbl, custom_schema)
        colspec_overrides = GendataFactory.eval_initial_colspecs(spark, colspec_overrides, derived_reference_tbl, compute_stats_table, source_reference_tbl)
        dspec_delta = (dg.DataGenerator(spark, name="wrapped_dbldatagen", rows=nrows, partitions=n_gen_partitions).withSchema(custom_schema))
        custom_schema_no_pk, dspec_delta = GendataFactory.primary_key_gen(primary_key, colspec_overrides, custom_schema, dspec_delta)
        df_delta = GendataFactory.infer_columns(custom_schema_no_pk, colspec_overrides, dspec_delta, default_colspecs)
        return (df_delta, custom_schema)
    
    def check_colspec_overrides_additional_fields(custom_schema, colspec_overrides):
        """
        Throws a warning if colspec_overrides has additional fields not present in custom_schema

        Args:
            custom_schema
            colspec_overrides
        Side effect:
            Send a warning if colsepc_overrides has additional field.
        """
        colspec_overrides_fields = set(colspec_overrides.keys())
        custom_schema_fields = set(field.name for field in custom_schema.fields)
        unspecified_fields = colspec_overrides_fields-custom_schema_fields
        
        if unspecified_fields:
            print("There are Fields in colspec_overrides not specified in custom_schema. These are the field names: " + str(unspecified_fields))
            logging.warn("There are Fields in colspec_overrides not specified in custom_schema. These are the field names: " + str(unspecified_fields))

    def createstatstableifnotexists(spark, compute_stats_table):
        """
        Creates a table for storing computed statistics if it does not already exist.

        Args:
            spark (SparkSession): The current Spark session.
            compute_stats_table (str): The name of the table to store computed statistics.

        Side effect:
            Creates a Delta table if it does not exist, designed to hold computed statistics.
        """
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {compute_stats_table} (TableName STRING, Field STRING, Type STRING, CategoricalValues ARRAY<STRING>, ContinuousValues STRUCT<min: DOUBLE, max: DOUBLE, ct: BIGINT>, InsertTs timestamp) USING delta")
        except AnalysisException as e:
            logging.error(f"An error occurred while creating the table: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

    def createupdatefieldstatstableifnotexists(spark, update_field_stats_table):
        """
        Creates a table for storing computed statistics if it does not already exist.

        Args:
            spark (SparkSession): The current Spark session.
            compute_stats_table (str): The name of the table to store computed statistics.

        Side effect:
            Creates a Delta table if it does not exist, designed to hold computed statistics.
        """
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {update_field_stats_table} (TableName STRING, Field STRING, UpdateValues ARRAY<STRING>, InsertTs timestamp) USING delta")
        except AnalysisException as e:
            logging.error(f"An error occurred while creating the table: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")