# this file is a kind of python startup module used for manual unit testing

import traceback
from extract.ext_countries import extraer_countries
from extract.ext_dates import extraer_dates
from extract.ext_stores import extraer_stores
from extract.per_staging import persistir_staging
from transform.tra_stores import transformar_stores
from load.load_stores import cargar_stores

try:
    cou = extraer_countries()
    #print (cou)
    persistir_staging(cou, 'ext_country')
    print("Datos de paises persitidos en Staging")
    dat = extraer_dates()
    #print (dat)
    persistir_staging(dat, 'ext_date')
    print("Datos de fechas persitidos en Staging") 
    sto =  extraer_stores()  
    persistir_staging(sto, 'ext_store')
    print("Datos de tiendas persitidos en Staging")    

    sto_tra = transformar_stores()
    persistir_staging(sto_tra, 'tra_store')
    print('Datos de tiendas transformados en Staging')

    cargar_stores()
    print('Datos de tiendas cargados al Sor')
    
except:
    traceback.print_exc()
finally:
    pass