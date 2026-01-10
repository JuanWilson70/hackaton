

import pandas as pd
import numpy as np
import pickle
import os


num=[1]

try:
    #La URL de  Bucket en OCI)
    archivo = 'https://objectstorage.sa-santiago-1.oraclecloud.com/p/fMawbmuwgiyBpMbGAFpoaLohXMs-ksdoxsGRhfR-JUA9E5FRPFttPIoEdyZAbQSI/n/axkzeuaauqax/b/bucket-datos-cliente/o/datos-clienteclientes.json'

except:
    print('Verifique la ruta del archivo de entrada')

try:
#Directorio de salidad de archivos
    # AHORA (Servidor)
    salida = './'  # Guarda los CSV en la misma carpeta donde está el script
    dir_modelo = './modelo_champion_GridSearch.pkl' # El nombre exacto de tu archivo subido
except:
    print('Verifique la ruta de los archivos de salida y del modelo')   

try:
    #Guarda tablas 
    def crear_tablas(tabla, salida, nombre):
        if not os.path.exists(salida+nombre+'.csv'):
            tabla.to_csv( salida + nombre + '.csv', index=False, encoding='utf-8')
        else:
            tabla.to_csv( salida + nombre + '.csv', index=False, mode='a', header=False, encoding='utf-8')


    guarda_tabla = lambda tabla, salida, nombre: tabla.to_csv( salida + nombre + '.csv', index=False, encoding='utf-8')


    def crea_tabla_ID():
        if not os.path.exists(salida+'ID_listos.csv'):
            # Si el archivo NO existe, lo creamos y escribimos algo
            with open(salida+'ID_listos.csv', 'w') as f:
                f.write("remainder__customerID")
    crea_tabla_ID()
except:
    print('Problemas con la creación de tablas')  

try:
#CONVIERTE JSON EN DF
    def extrae_datos(archivo):
        datos_list = list()
        datos_in=pd.read_json(archivo)  
        
        #desde la columna 2 en adelante normalizo los datos para cada columna creando una lista
        for col in datos_in.columns[2:]:
            v = pd.json_normalize(datos_in[col])  #Crea un DF para cada columna a normalizar dejando cada vaor unico dentro de la columna como una nueva columna
            v.columns = [f'{col}_{c}' for c in v.columns]    #Asigna el nombre a las nuevas columnas normailizadas (Nombre de la columna original+ nombre de columnas de nuevo DF)
            datos_list.append(v)

        # combino todo en un dataframe
        datos_normalized= pd.concat([datos_in.iloc[:, :2]] + datos_list, axis=1)  #dato todas las filas y columna 0 y 1 + datos_klist que son los normaliados, axis=1 se agrega al lado
        datos_entrada=datos_normalized.copy()
        return datos_normalized
except:
    print('Problemas con la extraccion de datos') 

try:
    #Se convierten columnas categóricas para que queden con valores 0 y 1
    def transforma_col(datos_tran):
        from sklearn.compose import make_column_transformer
        from sklearn.preprocessing import OneHotEncoder
        categoricas = ['account_Contract','account_PaymentMethod']

        one_hot_enc = make_column_transformer((OneHotEncoder(handle_unknown='ignore'),categoricas),remainder='passthrough',sparse_threshold=0,force_int_remainder_cols=False)

        datos_normalized = one_hot_enc.fit_transform(datos_tran)
        datos_normalized = pd.DataFrame(datos_normalized, columns=one_hot_enc.get_feature_names_out())
        
        return datos_normalized
except:
    print('Problemas con la transformación de datos') 
#entrada=extrae_datos()
#transforma_col(entrada)

try:
    def compara(entrada):
        d_normalized=entrada
        id_antiguos=pd.read_csv(salida+'ID_listos.csv')
        datos_nuevos=d_normalized[~d_normalized['remainder__customerID'].isin(id_antiguos['remainder__customerID'])]
        
        return datos_nuevos

           
    #compara(extrae_datos())
except:
    print('Problemas con la comparacion de datos') 


#Dato para ejecutar el while. Cuenta los cambios entre tablas
datos=compara(transforma_col(extrae_datos(archivo)))
cantidad=datos.shape[0]  
print(cantidad)  


while cantidad > 0:     

    try:
        def limpieza_de_datos(datos):
            datos_para_modelo=['remainder__customerID','remainder__customer_tenure'
                    ,'remainder__account_Charges.Total'
                    ,'remainder__account_Charges.Monthly'
                    ,'onehotencoder__account_Contract_Month-to-month'
                    ,'onehotencoder__account_PaymentMethod_Electronic check'
                    ,'remainder__internet_TechSupport'
                    ,'remainder__internet_OnlineSecurity'
                    ,'onehotencoder__account_Contract_Two year'
                    ,'remainder__account_PaperlessBilling'
                    ,'onehotencoder__account_Contract_One year']
            

            df_id=datos[datos_para_modelo]
            df_id=df_id.replace('No internet service','No')
            df_id=df_id.replace('No','0').replace('Yes','1')
            df_id.replace(r'^\s*$', np.nan, regex=True, inplace=True)
            df_id.replace('null', np.nan, inplace=True)
            df_id=df_id.dropna()
            df_id.reset_index(drop=True, inplace=True)
            df_id.reset_index(drop=True, inplace=True)
            df_id=df_id.rename(columns={'remainder__customerID': 'ID',
                                    'remainder__customer_tenure': 'Meses_contrato',
                                    'remainder__account_Charges.Total':'Total',
                                    'remainder__account_Charges.Monthly':'Factura_mensual',
                                    'onehotencoder__account_Contract_Month-to-month': 'Contrato_mensual',
                                    'onehotencoder__account_PaymentMethod_Electronic check': 'Pago_chequera_electronica',
                                    'remainder__internet_TechSupport': 'Soporte_tecnico',
                                    'remainder__internet_OnlineSecurity': 'Seguridad_online',
                                    'onehotencoder__account_Contract_Two year': 'Contrato_2_años',
                                    'remainder__account_PaperlessBilling': 'Factura_online',
                                    'onehotencoder__account_Contract_One year': 'Contrato_1_año'})    
            

            columna_excluir = 'ID'
            columnas_a_convertir = [col for col in df_id.columns if col != columna_excluir]
            df_id[columnas_a_convertir]=df_id[columnas_a_convertir].astype(float)

            df_modelo=df_id[columnas_a_convertir].copy()
            
            return df_id,df_modelo

        #datos=compara(transforma_col(extrae_datos()))
        #limpieza=limpieza_de_datos(datos)


        #tabla con IDs y variables
        #crear_tablas(limpieza[0], salida, 'Tabla_df_id')
    except:
        print('Problemas con la limpieza de datos') 

    #Trae Modelo

    try:
        def trae_modelo():    
            try:
                with open(dir_modelo, 'rb') as file:
                    model = pickle.load(file)
                print("¡Modelo cargado con éxito!")
            except Exception as e:
                print("Ocurrió un error al cargar el modelo:", str(e))
            return model

    
    except:
        print('Verifique la ruta de extracción del modelo') 

    #modelo=trae_modelo()


    try:
    #Genera predicciones
        def predicciones(datos,moledo):
            predic=pd.DataFrame()
            predic['predicciones']=modelo.predict(datos)
            predic['Probalilidad de Permanecer']=modelo.predict_proba(datos)[:,0]
            predic['Probalilidad de Renuncia']=modelo.predict_proba(datos)[:,1]
            predic['Probalilidad']=predic.apply(lambda row: row['Probalilidad de Permanecer'] 
                                                if row['Probalilidad de Permanecer'] > row['Probalilidad de Renuncia'] 
                                                else row['Probalilidad de Renuncia'], axis=1)
            return predic

        #modelo=trae_modelo()
        #datos_lim=limpieza[1]
        #predicciones(datos_lim,modelo)

        #Guarda DF con predicciones
        #crear_tablas(predicciones(datos,modelo), salida, 'Tabla_predicciones')

        #Concatena tablas por indice

    
        def pre_pro():
            ID_predicciones=pd.concat([limpieza[0].ID,predicciones(datos_mo,modelo)[['predicciones','Probalilidad']]],axis=1)
            return ID_predicciones

        #crear_tablas(pre_pro(), salida, 'Tabla_ID_predicciones')

        def ID_listos(todo):
            listos=todo['remainder__customerID']
            return listos

        #entrada=extrae_datos()
        #guarda_tabla(ID_listos(entrada), salida,'ID_listos')

        def mensaje(n):
            print(f'Se agregaron {n} predicciones nuevas a la base de datos')
            print(f'******                                         ********')
            print(f'******       Proceso finalizado con exito      ********')


        modelo=trae_modelo()

        entrada=extrae_datos(archivo)
        #transformados=transforma_col(entrada)
        #datos=compara(transformados)
        limpieza=limpieza_de_datos(datos)
        crear_tablas(limpieza[0], salida, 'Tabla_df_id')
        datos_mo=limpieza[1]
        pre=predicciones(datos_mo,modelo)
        crear_tablas(pre, salida, 'Tabla_predicciones')
        #limp_ID=limpieza[0]
        crear_tablas(pre_pro(), salida, 'Tabla_ID_predicciones')
        entrada=transforma_col(extrae_datos(archivo))
        guarda_tabla(ID_listos(entrada), salida,'ID_listos')
       
       #mensaje(cantidad)
        cantidad=0
        break

    except:
        
        cantidad=0
        break
 
else:
    

   # print(f'******     Hay {cantidad} datos nuevos para analizar     ********')
    print(f'******                                          ********')
    print(f'******     Proceso finalizado sin cambios       ********')

