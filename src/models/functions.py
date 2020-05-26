from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import when


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
    {target} as label
    from {output_name}
    ''')

    return finalised_data


def weight_balance(data, labels):
    ratio = 1 - data.groupby('label').count().collect()[0][1] / data.groupby('label').count().collect()[1][1]
    return when(labels == 1, ratio).otherwise(1 * (1 - ratio))


def evaluate_model(model, test_set, spark_session):
    test_set_name = 'test'
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    test_preds = model.transform(test_set)

    auc = evaluator.evaluate(test_preds)

    test_preds.createTempView(test_set_name)

    metrics = spark_session.sql(f'''
        select
        round({auc}*100, 3) as auc,
        round((true_positive / (true_positive + false_negative))*100, 3) as recall_pct,
        round((true_positive / (true_positive + false_positive))*100, 3) as precision_pct,
        *

        from 

        ( select
        round(avg(case when label=prediction then 1 else 0 end)*100, 3) as accuracy_pct,
        sum(case when label=1 and prediction=1 then 1 else 0 end) as true_positive,
        sum(case when label=0 and prediction=0 then 1 else 0 end) as true_negative,
        sum(case when label=0 and prediction=1 then 1 else 0 end) as false_positive,
        sum(case when label=1 and prediction=0 then 1 else 0 end) as false_negative

        from {test_set_name} )
            ''')

    metrics.show()

    return metrics
