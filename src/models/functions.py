from pyspark.ml.feature import VectorAssembler


def get_features(data, spark_session, target):
    output_name = f'features'
    features = VectorAssembler(
        inputCols=[var for var in data.columns if var not in target],
        outputCol=output_name
    )

    output = features.transform(data)

    output.createTempView(output_name)

    finalised_data = spark_session.sql(f'''
    select
    features,
    {target}
    from {output_name}
    ''')

    return finalised_data
