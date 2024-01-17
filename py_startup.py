import traceback
from extract.ext_cliente import extract_cliente
from extract.ext_contratacion import extract_contratacion
from extract.ext_proveedor import extract_proveedor
from extract.ext_servicio_publicitario import extract_servicio_publicitario
from extract.ext_ubicacion import extract_ubicacion
from extract.ext_venta import extract_venta
from extract.per_staging import persistir_staging
# from transform.tra_staging import transformar_staging
# from load.load_sor import cargar_sor

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

    # staging_df = transformar_staging()
    # persistir_staging(staging_df, 'tra_staging')
    # print('Datos de Staging transformados en Staging')

    # cargar_sor()
    # print('Datos de Staging cargados al Sor')

except:
    traceback.print_exc()
finally:
    pass
