import traceback
import pandas as pd
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig

def transformar_cliente(db_type='staging'):

    try:
        config = DatabaseConfig.DATABASES.get(db_type)

        if config is None:
            raise Exception(f"Invalid database type: {db_type}")

        # Iniciar conexión a la base de datos
        con_db = Db_Connection(db_type)
        ses_db = con_db.start()
        
        
        if ses_db == -1:
            raise Exception(f"El tipo de base de datos {type} no es válido")
        elif ses_db == -2:
            raise Exception("Error al establecer la conexión de pruebas")

        # Consulta SQL para realizar la transformación de la tabla tra_Cliente
        sql_stmt = """
            SELECT
                c.ID_Cliente,
                c.Nombre,
                c.Tipo,
                c.Sector,
                c.Email,
                c.Telefono,
                -- Columnas adicionales de tra_Tasa_Recompra y/o tra_Medicion_Volumenes_Venta
                -- Ajusta según tus necesidades
                tr.ClientesRecurrentes,
                mv.ClientesUnicos,
                mv.FacturacionTotal
            FROM
                ext_Cliente c
            LEFT JOIN
                tra_Tasa_Recompra tr ON (c.ID_Cliente = tr.ID_Cliente)
            LEFT JOIN
                tra_Medicion_Volumenes_Venta mv ON (c.ID_Cliente = mv.ID_Cliente)
        """

        # Realizar la transformación y obtener el DataFrame resultante
        cliente_tra = pd.read_sql(sql_stmt, ses_db)

        return cliente_tra

    except:
        traceback.print_exc()
    finally:
        pass
