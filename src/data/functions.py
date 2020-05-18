from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType


def drop_na_cols(data, pct):
    rows = data.count()
    null_counts = data.select(
        [F.count(
            F.when(
                F.isnull(col), col)
        ).alias(col) for col in data.columns]
    )

    null_counts = null_counts.toPandas()
    null_counts = (null_counts / rows).ge(pct).all()
    null_cols = null_counts[null_counts == True].keys()

    return data.select([col for col in data.columns if col not in null_cols])


def lower_case_cols(data):
    data_dtypes = {col[0]: col[1] for col in data.dtypes}

    for column in data_dtypes.keys():
        if data_dtypes[column] == 'string':
            data = data.withColumn(column, F.lower(F.col(column)))

    return data


def remove_whitespace(data):
    data_dtypes = {col[0]: col[1] for col in data.dtypes}

    for column in data_dtypes.keys():
        if data_dtypes[column] == 'string':
            data = data.withColumn(column, F.lower(F.col(column)))

    return data

def make_col_numeric(data, column):
    return data.withColumn(column, data[column].cast(IntegerType()))

def truncate_credit_line(data, column):
    return data.withColumn(column, F.split(F.col(column), '-')[1])