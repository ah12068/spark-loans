import streamlit as st
import pickle
from datetime import datetime, time
from variable_responses import *

st.warning(f"Load your ML model here")

st.title("A Machine Learning App")

st.sidebar.header("About")
st.sidebar.text(f"What does this app do?")

st.header("Project name here")

st.markdown("### Input your variables")

# Variables

v1 = st.radio("Variable 1", v1_vals)
v2 = st.selectbox("Variable 2", v2_vals)
v3 = st.multiselect("Variable 3", v3_vals)

if len(v3) == 0:
    st.error(f'Nothing is selected')

v4 = st.slider("Variable 4", v4_range_start, v4_range_end)

v5 = st.text_input("Variable 5")
v6 = st.text_area("Variable 6")

v7 = st.date_input("Variable 7", datetime.now())
v8 = st.time_input("Variable 8", time())


if st.button("Submit and Predict"):
    values_list = [v1, v2, v3, v4, v5, v6, v7, v8]
    X = []
    for element in values_list:
        if type(element) == list:
            for value in element:
                X.append(value)
        else:
            X.append(element)

    if '' in X:
        st.error(f"Empty values submitted")
    else:
        st.info(f"Variables Submitted: {X}")
        st.success(f"Use ML model to predict using {X}")
