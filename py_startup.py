import traceback
from extract.ext_cliente import extract_cliente
from extract.ext_contratacion import extract_contratacion
from extract.ext_proveedor import extract_proveedor
from extract.ext_servicio_publicitario import extract_servicio_publicitario
from extract.ext_ubicacion import extract_ubicacion
from extract.ext_venta import extract_venta
from extract.per_staging import persistir_staging
from transform.tra_contratacion import transformar_contratacion
from transform.tra_proveedor import transformar_proveedor
from transform.tra_servicio_publicitario import transformar_servicio_publicitario
from transform.tra_ubicacion import transformar_ubicacion
from transform.tra_venta import transformar_venta
from transform.tra_cliente import transformar_cliente
from transform.transformar import (
    transformar_tasa_recompra,
    transformar_datos_ventas,
    transformar_info_contratacion,
    transformar_medicion_volumenes_venta,

)
from load.load_cliente import cargar_dim_cliente
from load.load_contratacion import cargar_dim_contratacion
from load.load_proveedor import cargar_dim_proveedor
from load.load_servicio_publicitario import cargar_dim_servicio_publicitario
from load.load_ubicacion import cargar_dim_ubicacion
from load.load_venta import cargar_fact_venta



def persist_and_print(dataframe, table_name, message):
    persistir_staging(dataframe, table_name)
    print(f"{message} transformados y persistidos en Staging")

try:
    # Extract and persist data
    persist_and_print(extract_cliente(), 'ext_Cliente', 'Datos de Cliente')
    persist_and_print(extract_contratacion(), 'ext_Contratacion', 'Datos de Contratacion')
    persist_and_print(extract_proveedor(), 'ext_Proveedor', 'Datos de Proveedor')
    persist_and_print(extract_servicio_publicitario(), 'ext_Servicio_Publicitario', 'Datos de Servicio Publicitario')
    persist_and_print(extract_ubicacion(), 'ext_Ubicacion', 'Datos de Ubicacion')
    persist_and_print(extract_venta(), 'ext_Venta', 'Datos de Venta')

    # Transform and persist data
    persist_and_print(transformar_tasa_recompra(extract_cliente(), extract_venta()), 'tra_Tasa_Recompra', 'Datos de Tasa de Recompra')
    persist_and_print(transformar_datos_ventas(extract_servicio_publicitario(), extract_ubicacion(), extract_venta()), 'tra_Datos_Ventas', 'Datos de Datos de Ventas')
    persist_and_print(transformar_info_contratacion(extract_proveedor(), extract_contratacion()), 'tra_Info_Contratacion', 'Datos de Información de Contratación')
    persist_and_print(transformar_medicion_volumenes_venta(extract_venta()), 'tra_Medicion_Volumenes_Venta', 'Datos de Medición y Control de Volúmenes de Venta')
    persist_and_print(transformar_contratacion(), 'tra_Contratacion', 'Datos de Contratacion transformados en Staging')
    persist_and_print(transformar_proveedor(), 'tra_Proveedor', 'Datos de Proveedor')
    persist_and_print(transformar_servicio_publicitario(), 'tra_Servicio_Publicitario', 'Datos de Servicio Publicitario')
    persist_and_print(transformar_ubicacion(), 'tra_Ubicacion', 'Datos de Ubicacion')
    persist_and_print(transformar_venta(), 'tra_Venta', 'Datos de Venta')
    persist_and_print(transformar_cliente(), 'tra_Cliente', 'Datos de Cliente')

    cargar_dim_cliente()
    cargar_dim_contratacion()
    cargar_dim_proveedor()
    cargar_dim_servicio_publicitario()
    cargar_dim_ubicacion()
    cargar_fact_venta()


except Exception as e:
    traceback.print_exc()

finally:
    pass
