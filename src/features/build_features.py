import logging
import shutil as s
from os import path as p
from pyspark.sql import SparkSession
from constants import path, target, Id
from functions import *


def main(name='Loan_features'):
    logger = logging.getLogger(__name__)
    spark = SparkSession.builder.appName(f'{name}').getOrCreate()

    data = spark.read.csv(
        path,
        inferSchema=True,
        header=True
    )

    logger.info(f'Obtaining data types')
    nuniques = get_nuniques(data)
    binary_vars = [var for var in nuniques[nuniques.values == 2].index if var not in target]
    categorical_vars = [var for var in nuniques[nuniques.values <= 5].index if var not in target + binary_vars]
    numerical_vars = [var for var in data.columns if var not in Id + target + categorical_vars + binary_vars]

    logger.info(f'Encoding variables')
    for var in binary_vars:
        data = binarize(data, var)

    for var in categorical_vars:
        dummy_var = create_dummies(data, Id[0], var)

        data = data.join(dummy_var, data[Id[0]] == dummy_var[Id[0]])
        data = data.drop(dummy_var[Id[0]])
        data = data.drop(var)

    logger.info('Scaling numerical variables')
    for var in numerical_vars:
        data = min_max_scale(data, var)

    data = data.drop(*Id)

    output_filepath = f'../../data/processed/loans_spark'
    if p.exists(output_filepath):
        s.rmtree(output_filepath)

    data.repartition(1).write.format('csv').save(f"{output_filepath}", header='true')
    logger.info(f'Data exported to {output_filepath}')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables

    main()
