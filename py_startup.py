import traceback
from extract.ext_cliente import extract_cliente
from extract.ext_contratacion import extract_contratacion
from extract.ext_proveedor import extract_proveedor
from extract.ext_servicio_publicitario import extract_servicio_publicitario
from extract.ext_ubicacion import extract_ubicacion
from extract.ext_venta import extract_venta
from extract.per_staging import persistir_staging
from transform.transformar import (
    transformar_tasa_recompra,
    transformar_datos_ventas,
    transformar_info_contratacion,
    transformar_medicion_volumenes_venta,
)
from transform.tra_contratacion import transformar_contratacion
from transform.tra_proveedor import transformar_proveedor
from transform.tra_servicio_publicitario import transformar_servicio_publicitario
from transform.tra_ubicacion import transformar_ubicacion
from transform.tra_venta import transformar_venta

try:
    cliente_df = extract_cliente()
    persistir_staging(cliente_df, 'ext_Cliente')
    print("Datos de Cliente persistidos en Staging")

    contratacion_df = extract_contratacion()
    persistir_staging(contratacion_df, 'ext_Contratacion')
    print("Datos de Contratacion persistidos en Staging")

    proveedor_df = extract_proveedor()
    persistir_staging(proveedor_df, 'ext_Proveedor')
    print("Datos de Proveedor persistidos en Staging")

    servicio_df = extract_servicio_publicitario()
    persistir_staging(servicio_df, 'ext_Servicio_Publicitario')
    print("Datos de Servicio Publicitario persistidos en Staging")

    ubicacion_df = extract_ubicacion()
    persistir_staging(ubicacion_df, 'ext_Ubicacion')
    print("Datos de Ubicacion persistidos en Staging")

    venta_df = extract_venta()
    persistir_staging(venta_df, 'ext_Venta')
    print("Datos de Venta persistidos en Staging")

    # Transformar datos en staging
    tasa_recompra_df = transformar_tasa_recompra(cliente_df, venta_df)
    persistir_staging(tasa_recompra_df, 'tra_Tasa_Recompra')
    print("Datos de Tasa de Recompra transformados y persistidos en Staging")

    datos_ventas_df = transformar_datos_ventas(servicio_df, ubicacion_df, venta_df)
    persistir_staging(datos_ventas_df, 'tra_Datos_Ventas')
    print("Datos de Datos de Ventas transformados y persistidos en Staging")

    info_contratacion_df = transformar_info_contratacion(proveedor_df, contratacion_df)
    persistir_staging(info_contratacion_df, 'tra_Info_Contratacion')
    print("Datos de Información de Contratación transformados y persistidos en Staging")

    medicion_volumenes_df = transformar_medicion_volumenes_venta(venta_df)
    persistir_staging(medicion_volumenes_df, 'tra_Medicion_Volumenes_Venta')
    print("Datos de Medición y Control de Volúmenes de Venta transformados y persistidos en Staging")

    contratacion_tra = transformar_contratacion()
    persistir_staging(contratacion_tra, 'tra_Contratacion')
    print('Datos de contratacion transformados en Staging')
    
    # Transformar y persistir la tabla tra_Proveedor
    proveedor_tra = transformar_proveedor()
    persistir_staging(proveedor_tra, 'tra_Proveedor')
    print('Datos de Proveedor transformados y persistidos en Staging')

    # Transformar y persistir la tabla tra_Servicio_Publicitario
    servicio_publicitario_tra = transformar_servicio_publicitario()
    persistir_staging(servicio_publicitario_tra, 'tra_Servicio_Publicitario')
    print('Datos de Servicio Publicitario transformados y persistidos en Staging')


    # Transformar y persistir la tabla tra_Ubicacion
    ubicacion_tra = transformar_ubicacion()
    persistir_staging(ubicacion_tra, 'tra_Ubicacion')
    print('Datos de Ubicacion transformados y persistidos en Staging')


    # Transformar y persistir la tabla tra_Venta
    venta_tra = transformar_venta()
    persistir_staging(venta_tra, 'tra_Venta')
    print('Datos de Venta transformados y persistidos en Staging')
    
    # Transformar y persistir la tabla tra_Cliente
    cliente_tra = transformar_venta()
    persistir_staging(cliente_tra, 'tra_Cliente')
    print('Datos de Cliente transformados y persistidos en Staging')



    # cargar_sor()
    # print('Datos de Staging cargados al Sor')

except:
    traceback.print_exc()
finally:
    pass
