import plotly.express as px
from sqlalchemy import text
import sqlalchemy as sql
import pandas as pd


def connect_to_dataset() -> (pd.DataFrame, pd.DataFrame):
    db_connection_string = "mysql+pymysql://csadmin:2SZ#9!yHU!FFK4z0!B6o@util-rds-01.cit2wwhmueox.eu-west-2.rds.amazonaws.com/bloomberg_data_license"

    engine = sql.create_engine(db_connection_string)

    with engine.connect() as conn:
        requests = pd.read_sql(text(
            "SELECT DATE(timestamp) as timestamp, CASE WHEN SUBSTRING(d.name, 1, 6) = "
            "'bidask' THEN 'bidask' ELSE d.name END AS name, dataset_id as `dataset count` from requests r JOIN datasets "
            "d ON r.dataset_id=d.id WHERE success = 1"),
                               conn)

        engine.dispose()

    return requests


def request_chart():
    data = connect_to_dataset()
    data = data.groupby(by=["timestamp", "name"]).count().reset_index()
    return px.bar(data, x='timestamp', y='dataset count', color='name')
