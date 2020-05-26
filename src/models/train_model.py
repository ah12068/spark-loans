import logging
import shutil as s
from os import path as p
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from constants import path, target, output_path
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

    logger.info(f'Obtaining Weight balance')
    data = data.withColumn('weights', weight_balance(data, col('label')))

    logger.info(f'Create train and testing split 80-20')
    train, test = data.randomSplit([.8, .2], seed=1234)

    logger.info(f'Training and Optimising model')

    lr = LogisticRegression(
        featuresCol=data.columns[0],
        labelCol=data.columns[1],
        weightCol=data.columns[2],
        maxIter=100,
    )

    pipeline = Pipeline(stages=[lr])

    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.001, 0.01, 0.1, 1]) \
        .addGrid(lr.elasticNetParam, [0.001, 0.01, 0.1, 1]) \
        .build()

    model_tune = TrainValidationSplit(estimator=pipeline,
                                      estimatorParamMaps=paramGrid,
                                      evaluator=BinaryClassificationEvaluator(metricName='areaUnderPR'),
                                      trainRatio=0.8)

    model = model_tune.fit(train)

    metrics = evaluate_model(model, test, spark)

    model.bestModel.write().overwrite().save(output_path)
    metrics.toPandas().to_csv(f'{output_path}testset_metrics.csv')

    logger.info(f'Model and metrics exported to {output_path}')

    return model, metrics

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables

    main()
