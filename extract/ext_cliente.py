# extract/ext_cliente.py
import traceback
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig
import pandas as pd

def extract_cliente(db_type='oltp'):
    try:
        config = DatabaseConfig.DATABASES.get(db_type)

        if config is None:
            raise Exception(f"Invalid database type: {db_type}")

        con_db = Db_Connection(db_type)
        ses_db = con_db.start()

        if ses_db is None:
            raise Exception(f"Unable to establish a {db_type} database connection")

        cliente_data = pd.read_sql('SELECT * FROM Cliente', ses_db)
        return cliente_data

    except Exception as e:
        traceback.print_exc()
    finally:
        if ses_db:
            ses_db.dispose()
