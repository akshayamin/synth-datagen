from pyspark.sql import functions as F
from datetime import timedelta, date, time

## This function is used to optimize the number of partitions 
## based on the number of rows and the target rows per partition
def optimize_partitions(nrows: int, target_rows_per_partition: int) -> int:
    # Validate nrows => target_rows_per_partition if not, throw a warning
    if nrows <= target_rows_per_partition:
        print(
            f"""
            Warning: nrows ({nrows}) is less than target_rows_per_partition ({target_rows_per_partition}). 
            This may result in suboptimal partitioning. Defaulting gen partitions to 16
            """
        )

    partitions = max(nrows // target_rows_per_partition, 16)

    # Check for the upper limit on the number of partitions
    if partitions > 20000:
        print("Warning: The number of partitions exceeds 20,000. Consider adjusting the target rows per partition.")
        partitions = 20000  # Optionally enforce a hard limit

    return partitions

## This function is used to generate a random timestamp within a bounded range
## The bounded range is defined by input field_name plus some "day_variance"
def random_bounded_timestamp(field_name, day_variance=1):
    max_seconds_in_day = 24 * 60 * 60 * day_variance
    sql_expr = f"""
    CAST(
        date_format(
            date_trunc(
                'second', 
                from_unixtime(unix_timestamp({field_name}) + (rand() * ({max_seconds_in_day})))
            ), 
            'yyyy-MM-dd\\'T\\'HH:mm:ss'
        ) AS TIMESTAMP
    ) AS {field_name}
    """
    return sql_expr

#############################################
## DATE TIME HELPERS
def date_plus_n_days(dt: date, n_days: int = 1) -> str:
    return (dt + timedelta(days=n_days)).strftime("%Y-%m-%d")

def ts_plus_n_days(ts: time, n_days: int = 1) -> str:
    return (ts + timedelta(days=n_days)).strftime("%Y-%m-%d %H:%M:%S")

def dt_to_ts_str(dt: date) -> time:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

#############################################
## colspec builder helpers
def build_categorical_colspec(colname_list):
    return {col: {"derive": True, "type": "categorical", "field": col} for col in colname_list}

def build_continuous_colspec(colname_list):
    return {col: {"derive": True, "type": "continuous", "field": col} for col in colname_list}

def assemble_colspecs(*colspecs):
    colspec_assembler = {}
    for colspec in colspecs:
        if not isinstance(colspec, dict):
            raise ValueError("All arguments must be dictionaries")
        else:
            for col, spec in colspec.items():
                if not isinstance(spec, dict):
                    raise ValueError("All values must be dictionaries")
                else:
                    if col in colspec_assembler:
                        colspec_assembler[col].update(spec)
                    else:
                        colspec_assembler[col] = spec
    return colspec_assembler