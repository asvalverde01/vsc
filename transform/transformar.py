# Archivo: transformar/transformar.py
import pandas as pd

# Tasa de recompra de clientes por proveedor y periodos de tiempo:
def transformar_tasa_recompra(cliente_df, venta_df):
    try:
        merged_df = pd.merge(venta_df, cliente_df, how='inner', on='ID_Cliente')

        compras_por_cliente = merged_df.groupby(['ID_Cliente', 'ID_Proveedor']).size().reset_index(name='Compras')
        clientes_recurrentes = compras_por_cliente[compras_por_cliente['Compras'] > 1]
        tasa_recompra_df = clientes_recurrentes.groupby(['ID_Proveedor']).size().reset_index(name='ClientesRecurrentes')

        return tasa_recompra_df

    except Exception as e:
        print(f"Error en transformar_tasa_recompra: {e}")
        return None

# Datos de ventas por servicio publicitario y categoría de servicio, por ciudad o sector:
def transformar_datos_ventas(servicio_df, ubicacion_df, venta_df):
    try:
        merged_df = pd.merge(venta_df, servicio_df, how='inner', on='ID_Servicio')
        merged_df = pd.merge(merged_df, ubicacion_df, how='inner', on='ID_Ubicacion')

        datos_ventas_df = merged_df.groupby(['ID_Servicio', 'Categoria', 'Ciudad', 'Sector']).agg({'Monto': 'sum', 'ID_Venta': 'count'}).reset_index()
        datos_ventas_df.rename(columns={'ID_Venta': 'CantidadVentas'}, inplace=True)

        return datos_ventas_df

    except Exception as e:
        print(f"Error en transformar_datos_ventas: {e}")
        return None

# Información de contratación de servicios publicitarios por [Elevision]:
def transformar_info_contratacion(proveedor_df, contratacion_df):
    try:
        merged_df = pd.merge(contratacion_df, proveedor_df, how='inner', on='ID_Proveedor')
        info_contratacion_df = merged_df.groupby(['ID_Proveedor', 'Area_Elevision', 'Tipo_Servicio']).size().reset_index(name='Contrataciones')

        return info_contratacion_df

    except Exception as e:
        print(f"Error en transformar_info_contratacion: {e}")
        return None


# Medición y control de volúmenes de venta (cantidad de clientes y facturación), (año vs. año) dependiendo del sector y tipo de servicio publicitario:
def transformar_medicion_volumenes_venta(venta_df):
    try:
        # Asegúrate de que la columna 'Fecha' sea de tipo datetime
        venta_df['Fecha'] = pd.to_datetime(venta_df['Fecha'])

        # Añade las columnas 'Anio', 'Sector' y 'Tipo_Servicio' si no existen
        if 'Anio' not in venta_df.columns:
            venta_df['Anio'] = venta_df['Fecha'].dt.year
        if 'Sector' not in venta_df.columns:
            venta_df['Sector'] = ''  # Puedes ajustar esto según tus necesidades
        if 'Tipo_Servicio' not in venta_df.columns:
            venta_df['Tipo_Servicio'] = ''  # Puedes ajustar esto según tus necesidades

        # Incluye 'Sector' y 'Tipo_Servicio' en la operación de agrupación
        medicion_volumenes_df = venta_df.groupby(['Anio', 'Sector', 'Tipo_Servicio'], as_index=False).agg({'Monto': 'sum', 'ID_Cliente': 'nunique'})
        medicion_volumenes_df.rename(columns={'ID_Cliente': 'ClientesUnicos', 'Monto': 'FacturacionTotal'}, inplace=True)

        return medicion_volumenes_df

    except Exception as e:
        print(f"Error en transformar_medicion_volumenes_venta: {e}")
        return None
