import traceback
import pandas as pd
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig

def transformar_ubicacion(db_type='staging'):

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
        
        sql_stmt = """
            SELECT
                u.ID_Ubicacion,
                u.Ciudad,
                u.Sector,
                u.Latitud,
                u.Longitud,
                -- Columnas adicionales de tra_Datos_Ventas y/o tra_Medicion_Volumenes_Venta
                -- Ajusta según tus necesidades
                dv.Monto,
                dv.CantidadVentas,
                mv.FacturacionTotal,
                mv.ClientesUnicos
            FROM
                ext_Ubicacion u
            LEFT JOIN
                tra_Datos_Ventas dv ON (u.Ciudad = dv.Ciudad)
            LEFT JOIN
                tra_Medicion_Volumenes_Venta mv ON (u.Sector = mv.Sector)
        """

        ubicacion_tra = pd.read_sql(sql_stmt, ses_db)
        
        return ubicacion_tra

    except:
        traceback.print_exc()
    finally:
        pass
