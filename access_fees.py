from creds import db_connection_string
from typing import Dict, List, Union
from sqlalchemy import text
import sqlalchemy as sql
import pandas as pd
import datetime
import calendar


def dataset_connector() -> (pd.DataFrame, pd.DataFrame):
    engine = sql.create_engine(db_connection_string)

    with engine.connect() as conn:
        requests = pd.read_sql(text("SELECT timestamp, dataset_id FROM requests"), conn)
        datasets = pd.read_sql(text(
            "SELECT d.id as id, u.security as security, fl.data_category as data_category from datasets d "
            "JOIN field_lists fl ON d.field_listid=fl.id JOIN universes u ON u.id=d.universeid"),
            conn)

        engine.dispose()

    return datasets, requests


def create_december_data() -> pd.DataFrame:
    december_data = [{"data_category": "Security Master", "timestamp": "2022-12-01", "fee": 581.52},
                     {"data_category": "Derived", "timestamp": "2022-12-01", "fee": 279.30},
                     {"data_category": "Pricing", "timestamp": "2022-12-01", "fee": 155.32},
                     {"data_category": "Historical", "timestamp": "2022-12-01", "fee": 606.92}]

    return pd.DataFrame(december_data)


def calculate_number_of_new_accesses_per_day(datasets: pd.DataFrame, request_table: pd.DataFrame) -> pd.DataFrame:
    # need to get the securities by data category by day (join in mysql takes too long)
    datasets = pd.merge(request_table, datasets, how='inner', left_on='dataset_id', right_on='id')
    datasets['timestamp'] = datasets['timestamp'].dt.date

    # get number of new requests by day as well
    datasets = datasets.groupby(by=['data_category', 'security', 'timestamp'])['id'].apply(list).reset_index()

    # count the unique datasets excluding the initial one
    datasets['id'] = datasets['id'].apply(lambda x: len(set(x)) - 1)

    return datasets.loc[datasets['id'] > 0].copy().reset_index(drop=True)


def generate_new_rows(existing_fee_table: pd.DataFrame, fee_modifier: List[dict]):
    new_row_list = []
    # create business date range from max date of existing fee table and month end
    date_time_converted_object = max(existing_fee_table['timestamp'])
    end_of_month_by_day_of_month = \
        calendar.monthrange(date_time_converted_object.year, date_time_converted_object.month)[1]
    end_of_month_date = datetime.datetime.strptime(
        f"{date_time_converted_object.year}-{date_time_converted_object.month}-{end_of_month_by_day_of_month}",
        "%Y-%m-%d")
    business_day_range = pd.bdate_range(date_time_converted_object, end_of_month_date)[1:]

    # create rows to forward fill with (default is any that fall on the most recent day)

    default_rows_to_forward_fill = existing_fee_table.loc[
        existing_fee_table['timestamp'] == max(existing_fee_table['timestamp'])].copy().to_dict("records")

    # append any new rows added in the fee modifier
    for fee_addition in fee_modifier:
        if (security_number := fee_addition.get("Number of Securities")) > 1:
            for i in range(security_number):
                default_rows_to_forward_fill.append({"data_category": fee_addition["Data Category"],
                                                     "security": "dummy",
                                                     "id": fee_addition["Frequency per Day"]})

    for day in business_day_range:
        for rows in default_rows_to_forward_fill:
            rows['timestamp'] = day.to_pydatetime()
            new_row_list.append(rows)
    # append to existing_fee_table
    return pd.concat([existing_fee_table, pd.DataFrame(new_row_list)])


def category_access_fee_mapping(dataset_calling_table: pd.DataFrame, access_fee_mapping: Dict[str, float],
                                fee_modifier: Union[List[dict], None] = None) -> pd.DataFrame:
    # add fee modification
    if fee_modifier is not None:
        dataset_calling_table = generate_new_rows(dataset_calling_table, fee_modifier)

    # access the fee table
    dataset_calling_table['fee'] = dataset_calling_table['data_category'].apply(lambda x: access_fee_mapping[x])
    dataset_calling_table['fee'] = dataset_calling_table['id'] * dataset_calling_table['fee']
    dataset_calling_table['timestamp'] = dataset_calling_table['timestamp'].apply(
        lambda x: x.replace(day=1))
    return dataset_calling_table.groupby(by=["data_category", "timestamp"])['fee'].sum()


def return_access_fees(fee_modifier: Union[List[dict], None] = None) -> pd.DataFrame:
    # not really subject to change so can be hard coded dict
    access_mapping = {"Pricing": 0.01, "Security Master": 0.01, "Snapshot Pricing": 0.03, "Derived": 0.03}
    data, requests = dataset_connector()
    data = calculate_number_of_new_accesses_per_day(data, requests)

    data = category_access_fee_mapping(data, access_mapping, fee_modifier).reset_index()

    # streamlit is being really weird and return a date as a datetime
    data['timestamp'] = data['timestamp'].astype(str).apply(lambda x: x[:10])

    # add december data
    data = pd.concat([create_december_data(), data])
    data = pivot_table(data).fillna(0)

    return data


def pivot_table(table: pd.DataFrame):
    return pd.pivot_table(table, values="fee", index="data_category", columns='timestamp')
