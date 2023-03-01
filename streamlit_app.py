from format_table import format_values_in_fee_table, format_values_in_count_table
from dataset_breakdown import connect_to_dataset
from access_fees import return_access_fees
from unique_fees import return_unique_fees
from requests_plot import request_chart
import streamlit as st
import pandas as pd

if __name__ == '__main__':
    default_unique_dict = {"Derived": 0, "Pricing": 0, "Security Master": 0}
    default_fee_modifier = [{"Data Category": "Derived", "Number of Securities": 0, "Frequency per Day": 0}]

    with st.form('New Tickers'):
        data_category_1 = st.selectbox("Data Category", tuple(default_unique_dict.keys()))
        number_of_securities_1 = st.number_input("Number of Securities")
        frequency_1 = st.number_input("Frequency per Day")
        submission = st.form_submit_button('Submit')
        if submission:
            default_fee_modifier = [{"Data Category": data_category_1,
                                     "Number of Securities": int(number_of_securities_1),
                                     "Frequency per Day": int(frequency_1)}]
            default_unique_dict[data_category_1] += number_of_securities_1

    unique_fee_table, unique_fee_table_sec_counter, total_unique_fee, additional_unique_fees = return_unique_fees(
        default_unique_dict)
    access_fee_table, total_access_fee, additional_access_fee = return_access_fees(default_fee_modifier)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.write("<h5>Total Bloomberg Data License Fees</h5>", unsafe_allow_html=True)
        all_fees = total_access_fee + total_unique_fee
        if additional_access_fee or additional_unique_fees:
            st.write(f"${all_fees + additional_unique_fees + additional_access_fee:,.2f}")
            st.write(f"Additional Costs: ${additional_access_fee + additional_unique_fees:,.2f}")
        else:
            st.write(f"${all_fees:,.2f}")

    with col2:
        st.write("<h5>Total Accrued Access Fees</h5>", unsafe_allow_html=True)
        if additional_access_fee:
            st.write(f"${total_access_fee + additional_access_fee:,.2f}")
            st.write(f"Additional Costs: ${additional_access_fee:,.2f}")
        else:
            st.write(f"${total_access_fee:,.2f}")

    with col3:
        st.write("<h5>Total Accrued Unique Fees</h5>", unsafe_allow_html=True)
        if additional_unique_fees:
            st.write(f"${total_unique_fee + additional_unique_fees:,.2f}")
            st.write(f"Additional Costs: ${additional_unique_fees:,.2f}")
        else:
            st.write(f"${total_unique_fee:,.2f}")

    st.write("Unique Fees")
    st.table(unique_fee_table)

    st.write("Access Fees")
    st.table(access_fee_table)

    st.write("Distinct Number of Securities by Data Category and Month")
    st.table(unique_fee_table_sec_counter)

    reference_table = unique_fee_table_sec_counter[[unique_fee_table_sec_counter.columns[-1]]].copy().reset_index(
        drop=False)

    reference_table.columns = ['data_category', 'value']
    reference_table = {i['data_category']: int(i['value']) for i in reference_table.to_dict("records")}


    def concatenate_bounds(row):
        return f"{row['Lower Bound']} - {row['Upper Bound']}"


    fee_table = pd.read_csv("unique_fee_reference_sheet.csv")
    fee_table = fee_table.loc[fee_table['Data Category'] != 'Historical'].copy()
    fee_table['Monthly Cost'] = fee_table['Price per annum'] / 12
    fee_table['Band'] = fee_table.apply(concatenate_bounds, axis=1)

    fee_table = fee_table.drop(columns=['Price per annum', 'Lower Bound'])
    fee_table = pd.pivot_table(fee_table, values='Monthly Cost', index=['Band', 'Upper Bound'],
                               columns='Data Category').reset_index().rename_axis(None, axis=1)
    fee_table = fee_table.sort_values(by=['Upper Bound']).drop(columns=['Upper Bound']).reset_index(drop=True)

    for column in ['Derived', 'Pricing', 'Security Master']:
        fee_table[column] = fee_table[column].apply(lambda x: "${:,.2f}".format(x))

    fee_table = fee_table.set_index("Band")

    st.write("Bloomberg Bands")
    st.table(fee_table)

    st.write("Requests by day and dataset")
    st.plotly_chart(request_chart())
