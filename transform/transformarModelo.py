# Archivo: transformar/transformarModelo.py
import pandas as pd
from extract.per_staging import persistir_staging

# Cargar tablas desde el SOR
ext_Cliente = pd.read_sql("SELECT * FROM Dim_Cliente;", con=ses_db)
ext_Contratacion = pd.read_sql("SELECT * FROM Dim_Contratacion;", con=ses_db)
ext_Proveedor = pd.read_sql("SELECT * FROM Dim_Proveedor;", con=ses_db)
ext_Servicio_Publicitario = pd.read_sql("SELECT * FROM Dim_Servicio_Publicitario;", con=ses_db)
ext_Ubicacion = pd.read_sql("SELECT * FROM Dim_Ubicacion;", con=ses_db)
ext_Venta = pd.read_sql("SELECT * FROM Fact_Venta;", con=ses_db)

# Cargar tablas de transformación
tra_Datos_Ventas = pd.read_sql("SELECT * FROM tra_Datos_Ventas;", con=ses_db)
tra_Info_Contratacion = pd.read_sql("SELECT * FROM tra_Info_Contratacion;", con=ses_db)
tra_Medicion_Volumenes_Venta = pd.read_sql("SELECT * FROM tra_Medicion_Volumenes_Venta;", con=ses_db)
tra_Tasa_Recompra = pd.read_sql("SELECT * FROM tra_Tasa_Recompra;", con=ses_db)

# Realizar transformaciones y merges
# Puedes personalizar estas transformaciones según tus necesidades específicas

# Ejemplo de merge para ext_Cliente
ext_Cliente = pd.merge(ext_Cliente, tra_Tasa_Recompra, how='left', on='ID_Cliente')

# Ejemplo de merge para ext_Contratacion
ext_Contratacion = pd.merge(ext_Contratacion, tra_Info_Contratacion, how='left', on='ID_Proveedor')

# Ejemplo de merge para ext_Servicio_Publicitario
ext_Servicio_Publicitario = pd.merge(ext_Servicio_Publicitario, tra_Datos_Ventas, how='left', on='ID_Servicio')

# Ejemplo de merge para ext_Ubicacion
ext_Ubicacion = pd.merge(ext_Ubicacion, tra_Medicion_Volumenes_Venta, how='left', on='ID_Ubicacion')

# Puedes seguir agregando más transformaciones y merges según tus necesidades

# Persistir tablas en staging
persistir_staging(ext_Cliente, 'ext_Cliente')
persistir_staging(ext_Contratacion, 'ext_Contratacion')
persistir_staging(ext_Proveedor, 'ext_Proveedor')
persistir_staging(ext_Servicio_Publicitario, 'ext_Servicio_Publicitario')
persistir_staging(ext_Ubicacion, 'ext_Ubicacion')
persistir_staging(ext_Venta, 'ext_Venta')

print("Transformaciones y merges completados. Datos persistidos en Staging.")
