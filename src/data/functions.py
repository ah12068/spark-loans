from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from constants import categorical_vars, imputer_vars, new_vars, binary_class


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


def truncate_term(data, column):
    return data.withColumn(column, F.split(F.col(column), ' ')[1])


def categorise_employment_length(data, spark_session):
    name = categorical_vars['employment_length']
    data.createTempView(name)

    data = spark_session.sql(
        f"""

    select
    account_id,
    installment,
    loan_amount,
    interest_rate,
    term,
    purpose,
    issue_date,
    title,
    home_ownership,
    annual_income,

    case when employment_length in ('10+ years') then '10plus'
    when employment_length in ('< 1 year') then 'less1'
    when employment_length in ('1 year', '2 years', '3 years') then '1to3'
    when employment_length in ('4 years', '5 years', '6 years') then '4to6'
    when employment_length in ('7 years', '8 years', '9 years') then '7to9'
    else null
    end as employment_length,

    job_title,
    earliest_credit_line,
    public_records,
    delinquency_2y,
    inquiries_6m,
    open_accounts,
    debt_to_income,
    credit_card_usage,
    credit_card_balance,
    total_current_balance,
    nr_accounts,
    loan_status,
    amount_payed,
    year,
    district,
    postcode_district,
    credit_score

    from {name}
    """
    )

    return data


def categorise_home_ownership(data, spark_session):
    name = categorical_vars['home_ownership']
    data.createTempView(name)

    data = spark_session.sql(
        f"""
    select
    account_id,
    installment,
    loan_amount,
    interest_rate,
    term,
    purpose,
    issue_date,
    title,

    case when home_ownership in ('mortgage', 'rent', 'own') then home_ownership
    else 'other'
    end as home_ownership,

    annual_income,
    employment_length,
    job_title,
    earliest_credit_line,
    public_records,
    delinquency_2y,
    inquiries_6m,
    open_accounts,
    debt_to_income,
    credit_card_usage,
    credit_card_balance,
    total_current_balance,
    nr_accounts,
    loan_status,
    amount_payed,
    year,
    district,
    postcode_district,
    credit_score


    from {name}
    """
    )

    return data


def categorise_inquiry(data, spark_session):
    name = categorical_vars['inquiry']
    data.createTempView(name)

    data = spark_session.sql(
        f"""
    select
    
    account_id,
    installment,
    loan_amount,
    interest_rate,
    term,
    purpose,
    issue_date,
    title,
    home_ownership,
    annual_income,
    employment_length,
    job_title,
    earliest_credit_line,
    public_records,
    delinquency_2y,
    
    case when inquiries_6m = 0 then 'no_inquiry'
    when inquiries_6m = 1 then '1_inquiry'
    else '2plus_inquiry'
    end as inquiries_6m,
    
    open_accounts,
    debt_to_income,
    credit_card_usage,
    credit_card_balance,
    total_current_balance,
    nr_accounts,
    loan_status,
    amount_payed,
    year,
    district,
    postcode_district,
    credit_score
    
    from {name}
    """
    )
    return data


def categorise_purpose(data, spark_session):
    name = categorical_vars['purpose']
    data.createTempView(name)

    data = spark_session.sql(
        f"""
    select
    
    account_id,
    installment,
    loan_amount,
    interest_rate,
    term,
    
    case when purpose in ('debt_consolidation', 'credit_card') then purpose
    else 'other'
    end as purpose,
    
    issue_date,
    title,
    home_ownership,
    annual_income,
    employment_length,
    job_title,
    earliest_credit_line,
    public_records,
    delinquency_2y,
    inquiries_6m,
    open_accounts,
    debt_to_income,
    credit_card_usage,
    credit_card_balance,
    total_current_balance,
    nr_accounts,
    loan_status,
    amount_payed,
    year,
    district,
    postcode_district,
    credit_score
    
    from {name}
    """
    )

    return data


def impute_column(data, spark_session, grouper, column_to_fill):
    name = imputer_vars['total_current_balance']
    data.createTempView(name)

    medians = data.groupBy(grouper).agg(
        F.expr(f'percentile_approx({column_to_fill}, 0.5)').alias(f'median_{column_to_fill}'))
    medians_name = 'df_median'
    medians.createTempView(medians_name)

    imputed_data = spark_session.sql(f"""
    select

    account_id,
    installment,
    loan_amount,
    interest_rate,
    term,
    purpose,
    issue_date,
    title,
    home_ownership,
    annual_income,
    employment_length,
    job_title,
    earliest_credit_line,
    public_records,
    delinquency_2y,
    inquiries_6m,
    open_accounts,
    debt_to_income,
    credit_card_usage,
    credit_card_balance,

    case when total_current_balance is null and {medians_name}.median_{column_to_fill} is not null then {medians_name}.median_{column_to_fill} 
    else total_current_balance
    end as total_current_balance,

    nr_accounts,
    loan_status,
    amount_payed,
    year,
    {name}.district,
    postcode_district,
    credit_score

    from {name}
    left join {medians_name} on {medians_name}.district = {name}.district
    """
                                     )

    return imputed_data


def create_credit_age(data, spark_session, year):
    name = new_vars['credit_age_years']
    data.createTempView(name)

    data = spark_session.sql(f"""
    select
    *,

    ({year} - earliest_credit_line) as credit_age_years

    from {name}
    """
                             )

    return data


def create_binary_class(data, spark_session):
    name = binary_class['class']
    data.createTempView(name)

    data = spark_session.sql(f"""
    select

    *,

    case when loan_status in ('fully paid') then 0
    when loan_status in ('ongoing') then null

    when loan_status in ('default', 'charged_off', 'late (> 90 days)') then 1
    else null
    end as class

    from {name}
    """
                             )
    return data
