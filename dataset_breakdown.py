from creds import db_connection_string
from sqlalchemy import text
import sqlalchemy as sql
import pandas as pd


def connect_to_dataset() -> pd.DataFrame:
    engine = sql.create_engine(db_connection_string)

    with engine.connect() as conn:
        datasets = pd.read_sql(text("""SELECT 
  CASE WHEN SUBSTRING(d.name, 1, 6) = 'bidask' then 'bidask' else d.name end AS `Dataset name`, 
  GROUP_CONCAT(DISTINCT data_category SEPARATOR ', ') as `Data Categories`, 
  COUNT(DISTINCT u.security) AS `Number of Securities`,
	COUNT(DISTINCT d.name) as `Number of Daily Calls`
FROM 
  datasets d 
  JOIN universes u ON d.universeid = u.id 
  JOIN field_lists fl ON d.field_listid = fl.id 
	JOIN requests r ON r.dataset_id = d.id
GROUP BY 
  SUBSTRING(d.name, 1, 6)

"""), conn)

        engine.dispose()

    datasets = datasets.set_index('Dataset name')

    return datasets
