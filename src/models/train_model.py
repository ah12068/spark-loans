import logging
import shutil as s
from os import path as p
from pyspark.sql import SparkSession
from constants import path, target
from functions import *


def main(name='Loan_model'):
    logger = logging.getLogger(__name__)
    spark = SparkSession.builder.appName(f'{name}').getOrCreate()

    data = spark.read.csv(
        path,
        inferSchema=True,
        header=True
    )

    logger.info(f'Vectorising Features')

    data = get_features(data, spark, target)






if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables

    main()
