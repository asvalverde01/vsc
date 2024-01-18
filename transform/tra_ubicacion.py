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
                u.Ciudad,
                u.Sector,
                ROUND(COALESCE(SUM(ev.Monto), 0), 2) AS MontoTotal,
                COALESCE(COUNT(ev.ID_Venta), 0) AS VentasTotales,
                COUNT(DISTINCT CASE WHEN tr.ClientesRecurrentes > 0 THEN ev.ID_Cliente END) AS ClientesRecurrentes,
                COUNT(DISTINCT CASE WHEN tr.ClientesRecurrentes IS NULL THEN ev.ID_Cliente END) AS ClientesUnicosNuevos
            FROM
                ext_Ubicacion u
            LEFT JOIN
                ext_Venta ev ON u.ID_Ubicacion = ev.ID_Ubicacion
            LEFT JOIN
                tra_Tasa_Recompra tr ON ev.ID_Servicio = tr.ID_Proveedor
            GROUP BY
                u.Ciudad, u.Sector
        """

        ubicacion_tra = pd.read_sql(sql_stmt, ses_db)
        
        return ubicacion_tra

    except:
        traceback.print_exc()
        return None
