from creds import db_connection_string
from typing import List, Dict, Union
from sqlalchemy import text
import sqlalchemy as sql
import pandas as pd
import datetime

global total_unique_fees


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

            # messy way of doing a rolling 4month period
            for i in [i for i in range(index - 3, index + 1) if i >= 0]:
                cumulative_list_of_securities.extend(temp.at[i, 'security'])

            # add historical data, december bill for security master, hacky but necessary don't judge
            # December data unique fees stop at the end of March so here we go
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


def return_unique_fees(additional_category_input: Union[Dict[str, int], None] = None) -> (pd.DataFrame, pd.DataFrame, float, float):
    null_category_input = {"Derived": 0, "Pricing": 0, "Security Master": 0}
    requests_table, datasets_table = connect_to_dataset()
    full_request_table = squash_dataset_table_and_merge_with_request_table(datasets=datasets_table,
                                                                           requests=requests_table)
    unique_fee_table = calculate_rolling_cumulative_securities_by_month(full_request_table)

    final_fee_table = map_reference_fee_table_to_security_counter_table(unique_fee_table.copy(),
                                                                        return_fee_reference_table(),
                                                                        null_category_input)
    # categories specified to return
    if additional_category_input is not None:
        # negatives are not allowed
        assert not [v for k, v in additional_category_input.items() if v < 0]
    else:
        additional_category_input = null_category_input

    additional_fee_table = map_reference_fee_table_to_security_counter_table(unique_fee_table.copy(),
                                                                             return_fee_reference_table(),
                                                                             additional_category_input)

    final_fee_table, unique_fees_total, additional_fees = compare_fee_changes(final_fee_table, additional_fee_table)

    unique_fee_table_without_number_of_secs = final_fee_table.drop(columns=['number_of_cumulative_securities'])
    unique_fee_table_without_number_of_secs = pivot_table_fee(unique_fee_table_without_number_of_secs).fillna("$0.00")

    # get number of securities
    final_fee_table = final_fee_table[['timestamp', 'data_category', 'number_of_cumulative_securities']].copy()
    final_fee_table = pivot_table_sec_number(final_fee_table).fillna(0)

    if additional_fees:
        return unique_fee_table_without_number_of_secs, final_fee_table, unique_fees_total, additional_fees - unique_fees_total
    else:
        return unique_fee_table_without_number_of_secs, final_fee_table, unique_fees_total, 0


def pivot_table_fee(table: pd.DataFrame):
    return pd.pivot_table(table, values="unique_fee", index="data_category", columns='timestamp',
                          aggfunc=lambda i: ''.join(i))


def pivot_table_sec_number(table: pd.DataFrame):
    table['timestamp'] = table['timestamp'].astype(str)
    return pd.pivot_table(table, values="number_of_cumulative_securities", index="data_category", columns='timestamp',
                          aggfunc=lambda i: ''.join(i))


def compare_fee_changes(current_fee_table: pd.DataFrame, additional_fee_table: pd.DataFrame) -> (pd.DataFrame, float, float):
    unique_fees = sum(current_fee_table['unique_fee'])
    if current_fee_table.equals(additional_fee_table):
        current_fee_table['number_of_cumulative_securities'] = current_fee_table[
            'number_of_cumulative_securities'].apply(lambda i: f"{i:.0f}")
        current_fee_table['unique_fee'] = current_fee_table['unique_fee'].apply(lambda i: f"${i:,.2f}")
        return current_fee_table, unique_fees, 0

    current_fee_table_temp = current_fee_table.copy()
    additional_fee_table_temp = additional_fee_table.copy()

    # sum new fees
    additional_fees = sum(additional_fee_table['unique_fee'])

    combined_fee_table = pd.merge(current_fee_table_temp, additional_fee_table_temp, how='inner',
                                  right_on=['data_category', 'timestamp'],
                                  left_on=['data_category', 'timestamp'])

    def track_differences_and_concat_strings_number_of_secs(row):
        if row['number_of_cumulative_securities_x'] != row['number_of_cumulative_securities_y']:
            return f"{int(row['number_of_cumulative_securities_x'])} + {int(row['number_of_cumulative_securities_y'] - row['number_of_cumulative_securities_x'])}"
        else:
            return f"{int(row['number_of_cumulative_securities_x'])}"

    def track_differences_and_concat_strings_fees(row):
        if row['unique_fee_x'] != row['unique_fee_y']:
            return f"${row['unique_fee_x']:,.2f} + ${row['unique_fee_y'] - row['unique_fee_x']:,.2f}"
        else:
            return f"${row['unique_fee_x']:,.2f}"

    combined_fee_table['number_of_cumulative_securities'] = combined_fee_table.apply(
        track_differences_and_concat_strings_number_of_secs, axis=1)
    combined_fee_table['unique_fee'] = combined_fee_table.apply(
        track_differences_and_concat_strings_fees, axis=1)

    return combined_fee_table[['data_category', 'timestamp', 'number_of_cumulative_securities',
                               'unique_fee']].copy(), unique_fees, additional_fees
