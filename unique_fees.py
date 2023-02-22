from creds import db_connection_string
from typing import List, Dict, Union
from sqlalchemy import text
import sqlalchemy as sql
import pandas as pd
import datetime


def connect_to_dataset() -> (pd.DataFrame, pd.DataFrame):
    engine = sql.create_engine(db_connection_string)

    with engine.connect() as conn:
        requests = pd.read_sql(text("SELECT timestamp, dataset_id from requests WHERE success = 1"), conn)
        datasets = pd.read_sql(text(
            "SELECT d.id as id, u.security as security, fl.data_category as data_category from datasets d "
            "JOIN field_lists fl ON d.field_listid=fl.id JOIN universes u ON u.id=d.universeid"),
            conn)

        engine.dispose()

    return requests, datasets


def create_december_data() -> pd.DataFrame:
    december_data = [{"data_category": "Derived", "timestamp": datetime.datetime(2022, 12, 1).date(),
                      "number_of_cumulative_securities": 1091},
                     {"data_category": "Pricing", "timestamp": datetime.datetime(2022, 12, 1).date(),
                      "number_of_cumulative_securities": 199},
                     {"data_category": "Security Master", "timestamp": datetime.datetime(2022, 12, 1).date(),
                      "number_of_cumulative_securities": 2661},
                     {"data_category": "Historical", "timestamp": datetime.datetime(2022, 12, 1).date(),
                      "number_of_cumulative_securities": 1756}]
    return pd.DataFrame(december_data)


def squash_dataset_table_and_merge_with_request_table(datasets: pd.DataFrame, requests: pd.DataFrame) -> pd.DataFrame:
    # squash table
    squashed_table = datasets.groupby(by=['data_category', 'id'])['security'].apply(list).reset_index()

    request_merged_table = pd.merge(requests, squashed_table, how='inner', left_on='dataset_id', right_on='id')

    # create month year tag by getting first day of month
    request_merged_table['timestamp'] = request_merged_table['timestamp'].apply(
        lambda x: x.to_pydatetime().replace(day=1).date())

    # create list of lists series
    request_merged_table = request_merged_table.groupby(by=['data_category', 'timestamp'])['security'].apply(
        list).reset_index()

    # flatten list of securities
    request_merged_table['security'] = request_merged_table['security'].apply(lambda x: flatten_list(x))

    return request_merged_table


def flatten_list(list_of_lists: List[List[str]]) -> List[str]:
    new_list = []
    for i in list_of_lists:
        for j in i:
            new_list.append(j)
    return new_list


def calculate_rolling_cumulative_securities_by_month(request_merged_table: pd.DataFrame) -> pd.DataFrame:
    df_list = []
    for data_category in pd.unique(request_merged_table['data_category']):
        temp = request_merged_table.loc[request_merged_table['data_category'] == data_category].copy() \
            .sort_values(by=['timestamp']).reset_index(drop=True)
        for index, row in temp.iterrows():
            cumulative_list_of_securities = []
            for i in [i for i in range(index - 3, index + 1) if i >= 0]:
                cumulative_list_of_securities.extend(temp.at[i, 'security'])

            # add historical data, december bill for security master, hacky but necessary don't judge
            if row['timestamp'] <= datetime.datetime(2023, 3, 31).date() and row['data_category'] == 'Security Master':
                temp.at[index, 'number_of_cumulative_securities'] = len(set(cumulative_list_of_securities)) + 2440
            else:
                temp.at[index, 'number_of_cumulative_securities'] = len(set(cumulative_list_of_securities))

        temp = temp.drop(columns=['security'])
        df_list.append(temp)

    return pd.concat(df_list).reset_index(drop=True)


def return_fee_reference_table() -> List[dict]:
    return pd.read_csv("unique_fee_reference_sheet.csv").to_dict("records")


def map_reference_fee_table_to_security_counter_table(security_counter: pd.DataFrame, fee_reference_table: List[dict],
                                                      additional_category_input: Dict[str, int]) -> pd.DataFrame:
    # fetch any modifications and add the distinct number on
    def modify_counter(row) -> int:
        if row['timestamp'] == max(security_counter['timestamp']):
            number_of_secs = row['number_of_cumulative_securities'] + additional_category_input[row['data_category']]
        else:
            number_of_secs = row['number_of_cumulative_securities']

        return number_of_secs

    # fetch the price of the security counter by data category from records
    def pull_counter_and_get_price(row) -> float:
        return [i for i in fee_reference_table if row['data_category'] == i.get("Data Category") and (
                i.get("Upper Bound") >= row['number_of_cumulative_securities'] >= i.get("Lower Bound"))][0].get(
            "Price per annum") / 12

    security_counter['number_of_cumulative_securities'] = security_counter.apply(modify_counter, axis=1)

    # add December data
    security_counter = pd.concat([create_december_data(), security_counter])
    security_counter['unique_fee'] = security_counter.apply(pull_counter_and_get_price, axis=1)

    return security_counter


def return_unique_fees(additional_category_input: Union[Dict[str, int], None] = None) -> (pd.DataFrame, pd.DataFrame):
    requests_table, datasets_table = connect_to_dataset()
    full_request_table = squash_dataset_table_and_merge_with_request_table(datasets=datasets_table,
                                                                           requests=requests_table)
    unique_fee_table = calculate_rolling_cumulative_securities_by_month(full_request_table)

    fee_reference_table = return_fee_reference_table()

    if additional_category_input is None:
        additional_category_input = {"Derived": 0, "Pricing": 0, "Security Master": 0}
    else:
        assert not [v for k, v in additional_category_input.items() if v < 0]

    unique_fee_table = map_reference_fee_table_to_security_counter_table(unique_fee_table, fee_reference_table,
                                                                         additional_category_input)

    unique_fee_table_without_number_of_secs = unique_fee_table.drop(columns=['number_of_cumulative_securities'])
    unique_fee_table_without_number_of_secs = pivot_table_fee(unique_fee_table_without_number_of_secs).fillna(0)

    # get number of securities
    unique_fee_table = unique_fee_table[['timestamp', 'data_category', 'number_of_cumulative_securities']].copy()
    unique_fee_table['number_of_cumulative_securities'] = unique_fee_table['number_of_cumulative_securities']
    unique_fee_table = pivot_table_sec_number(unique_fee_table).fillna(0)

    return unique_fee_table_without_number_of_secs, unique_fee_table


def pivot_table_fee(table: pd.DataFrame):
    return pd.pivot_table(table, values="unique_fee", index="data_category", columns='timestamp')


def pivot_table_sec_number(table: pd.DataFrame):
    return pd.pivot_table(table, values="number_of_cumulative_securities", index="data_category", columns='timestamp')
