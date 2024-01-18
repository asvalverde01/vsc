import traceback
import pandas as pd
from util.db_connection import Db_Connection
from util.db_config import DatabaseConfig

def transformar_venta(db_type='staging'):

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

        # Consulta SQL ajustada para realizar la transformación de la tabla tra_Venta
        sql_stmt = """
            SELECT
                c.Nombre AS Cliente,
                p.Nombre AS Proveedor,
                s.Nombre AS Servicio,
                u.Ciudad AS Ciudad,
                u.Sector AS Sector,
                v.ID_Factura,
                v.Fecha,
                v.Monto,
                v.DuracionAnuncio,
                -- Columnas adicionales de tra_Tasa_Recompra y/o tra_Medicion_Volumenes_Venta
                -- Ajusta según tus necesidades
                tr.ClientesRecurrentes
            FROM
                ext_Venta v
            LEFT JOIN
                tra_Tasa_Recompra tr ON (v.ID_Proveedor = tr.ID_Proveedor)
            LEFT JOIN
                tra_Medicion_Volumenes_Venta mv ON (YEAR(v.Fecha) = mv.Anio)
            LEFT JOIN
                ext_Cliente c ON (v.ID_Cliente = c.ID_Cliente)
            LEFT JOIN
                ext_Proveedor p ON (v.ID_Proveedor = p.ID_Proveedor)
            LEFT JOIN
                ext_Servicio_Publicitario s ON (v.ID_Servicio = s.ID_Servicio)
            LEFT JOIN
                ext_Ubicacion u ON (v.ID_Ubicacion = u.ID_Ubicacion)
        """

        # Realizar la transformación y obtener el DataFrame resultante
        venta_tra = pd.read_sql(sql_stmt, ses_db)

        return venta_tra

    except:
        traceback.print_exc()
        return None
