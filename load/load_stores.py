import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_stores ():

    try:
        type = 'mysql'
        host = '10.10.10.2'
        port = '3306'
        user = 'dwh'
        pwd = 'elcaro_4U'
        db = 'staging'

        con_db_stg = Db_Connection(type, host, port, user, pwd, db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"El tipo de base de datos {type} no es válido")
        elif ses_db_stg == -2:
            raise Exception("Error al establecer la conexión de pruebas")        
        
        sql_stmt = "SELECT store_id, name, city, country \
                    FROM tra_store"
        stores_tra = pd.read_sql(sql_stmt, ses_db_stg)
        
        type = 'mysql'
        host = '10.10.10.2'
        port = '3306'
        user = 'dwh'
        pwd = 'elcaro_4U'
        db = 'sor'

        con_db_sor = Db_Connection(type, host, port, user, pwd, db)
        ses_db_sor = con_db_sor.start()
        if ses_db_sor == -1:
            raise Exception(f"El tipo de base de datos {type} no es válido")
        elif ses_db_sor == -2:
            raise Exception("Error al establecer la conexión de pruebas") 
        
        dim_sto_dict = {
            "store_bk":[],
            "name":[],
            "city":[],
            "country":[],
        }

        if not stores_tra.empty:
            for bk,nam,cit,cou \
                in zip(stores_tra['store_id'], stores_tra['name'], stores_tra['city'], stores_tra['country']):
                dim_sto_dict['store_bk'].append(bk)
                dim_sto_dict['name'].append(nam)
                dim_sto_dict['city'].append(cit)
                dim_sto_dict['country'].append(cou)
        
        if dim_sto_dict['store_bk']:
            df_dim_store = pd.DataFrame(dim_sto_dict)
            df_dim_store.to_sql('dim_store', ses_db_sor, if_exists='append', index=False)

    except:
        traceback.print_exc()
    finally:
        pass
