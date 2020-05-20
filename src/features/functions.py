from constants import (
    Id,
    target,
    distinct_count
)
from pyspark.ml.feature import StringIndexer, MinMaxScaler, VectorAssembler
from pyspark.sql import functions as F


def get_nuniques(data):
    nuniques = data.agg(*(F.countDistinct(F.col(column)).alias(column) for column in data.columns))
    nuniques = nuniques.toPandas().transpose()
    nuniques.columns = [distinct_count]

    return nuniques


def binarize(data, column):
    output_name = f'binary_{column}'

    encoder = StringIndexer(
        inputCol=column,
        outputCol=output_name
    )

    model = encoder.fit(data)
    binarized_data = model.transform(data)

    binarized_data = binarized_data.drop(column)

    return binarized_data


def create_dummies(data, identifier, column):
    categories = data.select(column).distinct().rdd.flatMap(lambda x: x).collect()

    exprs = [F.when(F.col(column) == category, 1).otherwise(0).alias(f'{column}_{category}') for category in categories]

    return data.select(identifier, *exprs)


def min_max_scale(data, column):
    feature_name = f'feature_{column}'
    feature_assembler = VectorAssembler(
        inputCols=[column],
        outputCol=feature_name
    )
    scaler = MinMaxScaler(
        inputCol=feature_name,
        outputCol=f'scaled_{column}'
    )
    assembler = feature_assembler.transform(data)
    model = scaler.fit(assembler)
    encoded_data = model.transform(assembler)
    encoded_data = encoded_data.drop(*[column, feature_name])

    return encoded_data
