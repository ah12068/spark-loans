import logging
import shutil as s
from os import path as p
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
from constants import path, target, output_path
from functions import *


def main(name='Predict_model', data):
    spark = SparkSession.builder.appName(f'{name}').getOrCreate()
    model = PipelineModel.load(outout_path)

    # Use make_dataset.py -> build_features.py -> get_features() -> just 'features' column containing vector of features
    # to format, clean data, it is same thing without 'class' variable

    data_to_pred = spark.read.csv(
        path,
        inferSchema=True,
        header=True
    )

    predictions = model.transform(data_to_pred)

    return predictions



if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables

    main()
