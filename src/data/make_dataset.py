import logging
from os import path as p
import shutil as s
from pyspark.sql import SparkSession
from constants import path, columns_to_drop
from functions import *


def main(name='Clean_loans'):
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
    data = truncate_credit_line(data, 'earliest_credit_line')
    data = truncate_term(data, 'term')
    data = make_col_numeric(data, 'earliest_credit_line')
    data = make_col_numeric(data, 'term')
    data = make_col_numeric(data, 'annual_income')
    data = make_col_numeric(data, 'credit_score')


    logger.info(f'Re-categorising categorical variables')
    data = categorise_employment_length(data, spark)
    data = categorise_home_ownership(data, spark)
    data = categorise_inquiry(data, spark)
    data = categorise_purpose(data, spark)

    logger.info(f'Imputing variables')
    data = impute_column(data, spark, 'district', 'total_current_balance')

    logger.info(f'Creating variables')
    data = create_credit_age(data, spark, 2015)
    data = create_binary_class(data, spark)

    logger.info(f'Dropping redundant variables and NA samples')
    data = data.drop(*columns_to_drop['drop'])
    data = data.na.drop()

    new_size = data.count()

    logger.info(f'Clean data rows: {new_size} '
                f'Row Difference: {initial_size - new_size}')

    output_filepath = f'../../data/interim/loans_clean_spark'

    if p.exists(output_filepath):
        s.rmtree(output_filepath)

    data.repartition(1).write.format('csv').save(f"{output_filepath}", header='true')
    logger.info(f'Clean data exported to {output_filepath}')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables

    main()
