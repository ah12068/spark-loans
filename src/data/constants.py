path = f'../../data/raw/loans_raw.csv'

categorical_vars = {
    'home_ownership': 'df_ho',
    'employment_length': 'df_el',
    'inquiry': 'df_inq',
    'purpose': 'df_purpose',
}

imputer_vars = {
    'total_current_balance': 'df_tcb'
}

new_vars = {
    'credit_age_years': 'df_credit_age'
}

binary_class = {
    'class': 'df_binary_class'
}