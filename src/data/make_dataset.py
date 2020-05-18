import logging
from pyspark.sql import SparkSession
from constants import *
from functions import *


def main(name):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    spark = SparkSession.builder.appName(f'{name}').getOrCreate()

    data = spark.read.csv(
        path,
        inferSchema=True,
        header=True
    )
    initial_size = data.count()

    logger.info(f'Dropping cols w/ >= 50% nans')
    data = drop_na_cols(data=data, pct=0.5)

    logger.info(f'Cleaning text based data')
    data = lower_case_cols(data)
    data = remove_whitespace(data)

    logger.info(f'Cleaning data types')
    data = make_col_numeric(data, 'annual_income')
    data = make_col_numeric(data, 'credit_score')
    data = truncate_credit_line(data, 'earliest_credit_line')








if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables

    main()
