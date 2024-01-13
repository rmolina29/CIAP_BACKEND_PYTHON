from fastapi import  UploadFile
import pandas as pd
import math
from app.database.db import session
from app.parametros.cliente.model.cliente_model import ProyectoCliente
from typing import List
# from sqlalchemy import and_

class Cliente:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        # data para la comparacion de informacion sacandola de la base de datos
        self.__obtener_clientes_existente = self.obtener()
        # data que se usara para hacer la comparacion informacion que envia el usuario a traves del excel
        self.__data_usuario_cliente = self.validacion_informacion_cliente_nit()['cliente_filtro_excel']
        
        
    def validacion_existe_informacion(self)->bool:
        return len(self.__data_usuario_cliente) > 0 or len(self.__obtener_clientes_existente)>0

    def __proceso_de_informacion_estructuracion(self):
        df = pd.read_excel(self.__file.file)
        # Imprimir las columnas reales del DataFrame
        selected_columns = ["ID Cliente (ERP)", "Nombre", "NIT"]

        selected_data = df[selected_columns]
        
          # Cambiar los nombres de las columnas
        selected_data = selected_data.rename(
            columns={
               "ID Cliente (ERP)": "cliente_id_erp", 
                "Nombre": "razon_social", 
                "NIT": "identificacion"
            }
        )
        
        selected_data["cliente_id_erp"] = selected_data["cliente_id_erp"].str.lower()
        selected_data["razon_social"] = selected_data["razon_social"].str.lower()
        
        df_filtered = selected_data.dropna()
        
        duplicados_unidad_erp = df_filtered.duplicated(subset='cliente_id_erp', keep=False)
        duplicados_razon_social = df_filtered.duplicated(subset='razon_social', keep=False)
        # Filtrar DataFrame original
        resultado = df_filtered[~(duplicados_unidad_erp | duplicados_razon_social)].to_dict(orient='records')
        duplicados = df_filtered[(duplicados_unidad_erp | duplicados_razon_social)].to_dict(orient='records')
        
        duplicated = pd.DataFrame(duplicados)
        
        if duplicated.isnull().any(axis=1).any():
            lista_gerencias = []
        else:
            lista_gerencias = [{**item, 'identificacion': int(item['identificacion'])} if isinstance(item.get('identificacion'), (int, float)) and not math.isnan(item.get('identificacion')) else item for item in duplicados]
        
        return {'resultado':resultado,'duplicados':lista_gerencias}
    
    def transacciones(self):
        
        data_excel_filtro = self.validacion_informacion_cliente_nit()
        obtener_duplicados = self.__proceso_de_informacion_estructuracion()
        registro_clientes = self.filtro_clientes_nuevos()
        actualizar_clientes = self.filtro_cliente_actualizar()
                
        log_transaccion_registro_gerencia = {
                "log_transaccion_excel": {
                   'excepciones':self.obtener_excepciones_datos_unicos(),
                   'sin_cambios':self.obtener_no_sufrieron_cambios(),
                   'nit_invalidos':data_excel_filtro['log'],
                   'registo':self.insertar_informacion(registro_clientes),
                   'actualizar':self.actualizar_informacion(actualizar_clientes),
                   'duplicados':obtener_duplicados['duplicados'],
                }
        }
        
        return log_transaccion_registro_gerencia
    
    
    def obtener(self) -> List:
        informacion_cliente= session.query(ProyectoCliente).all()
        # Convertir lista de objetos a lista de diccionarios
        cliente = [cliente.to_dict() for cliente in informacion_cliente]
        return cliente
    
    def validacion_informacion_cliente_nit(self):
        try:
            resultado_estructuracion = self.__proceso_de_informacion_estructuracion()
            cliente_log, cliente_filtro_excel = [], []
            [(cliente_filtro_excel if isinstance(item.get('identificacion'), (int,float)) else cliente_log).append(item) for item in resultado_estructuracion['resultado']]
            return {'log': cliente_log, 'cliente_filtro_excel': cliente_filtro_excel}
        except Exception as e:
            raise (f"Error al realizar la comparación: {str(e)}") from e
    
    def filtro_clientes_nuevos(self):
        
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_clientes = pd.DataFrame(self.__data_usuario_cliente)
            df_obtener_clientes_existentes = pd.DataFrame(self.__obtener_clientes_existente)
            
            df_clientes["cliente_id_erp"] = df_clientes["cliente_id_erp"].str.lower()
            df_clientes["razon_social"] = df_clientes["razon_social"].str.lower()

            df_obtener_clientes_existentes["cliente_id_erp"] = df_obtener_clientes_existentes["cliente_id_erp"].str.lower()
            df_obtener_clientes_existentes["razon_social"] = df_obtener_clientes_existentes["razon_social"].str.lower()
                
            resultado = df_clientes[
                    ~df_clientes.apply(lambda x: 
                        ((x['cliente_id_erp'] in set(df_obtener_clientes_existentes['cliente_id_erp'])) 
                         or 
                         (x['razon_social'] in set(df_obtener_clientes_existentes['razon_social']))
                         or
                         (x['identificacion'] in set(df_obtener_clientes_existentes['identificacion']))
                         ), axis=1)
                ]
                                
            # Resultado final
            clientes_registro = resultado.to_dict(orient='records')
            
            # filtro_registro = self.filtro_identificacion_unica(
            #     clientes_registro
            # )
            # if len(filtro_registro) == 0:
            #     return clientes_registro
            
            # df_registro = pd.DataFrame(filtro_registro)
            # registros = df_registro.drop(columns=['id']) 
            # registros_unidad_organizativa = registros.to_dict(orient='records')
            
            # print(registros_unidad_organizativa)
        else:
            clientes_registro = []
            
        return clientes_registro
    
    def filtro_cliente_actualizar(self):
        
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            
            df_clientes,df_obtener_clientes_existentes = self.obtener_data_serializada()

            resultado = pd.merge(
                df_clientes[['cliente_id_erp','razon_social','identificacion']],
                df_obtener_clientes_existentes[['id','cliente_id_erp','razon_social','identificacion']],
                left_on=['cliente_id_erp'],
                right_on=['cliente_id_erp'],
                how='inner',
            )
            
            resultado_final = resultado[['id', 'cliente_id_erp', 'razon_social_x', 'identificacion_x']].rename(columns={'razon_social_x':'razon_social','identificacion_x':'identificacion'})
            
            cliente_actualizar = resultado_final[
                          ~resultado_final.apply(lambda x: (
                            (x['razon_social'] in set(df_obtener_clientes_existentes['razon_social'])) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['razon_social'] == x['razon_social'], 'cliente_id_erp'].values[0]) 
                            or
                            (x['identificacion'] in set(df_obtener_clientes_existentes['identificacion'])) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['identificacion'] == x['identificacion'], 'cliente_id_erp'].values[0])
                        ), axis=1)
                    ]
            
            resultado_actualizacion = cliente_actualizar.to_dict(orient='records')
            cliente_filtro = self.obtener_no_sufrieron_cambios()
            
            if len(cliente_filtro) != 0:
                df_cliente = pd.DataFrame(cliente_filtro)
                df_filtrado = cliente_actualizar[~cliente_actualizar.isin(df_cliente.to_dict('list')).all(axis=1)]
                return df_filtrado.to_dict(orient='records')
            
            return resultado_actualizacion
        else:
            resultado_actualizacion = []
            
        return resultado_actualizacion
            
    def obtener_no_sufrieron_cambios(self):
        validacion = self.validacion_existe_informacion()
        if validacion:
            
            df_clientes,df_obtener_clientes_existentes = self.obtener_data_serializada()
            
            resultado = pd.merge(df_clientes, df_obtener_clientes_existentes, how='inner', on=['cliente_id_erp', 'razon_social', 'identificacion'])
            
            clientes_sin_ningun_cambio = resultado.to_dict(orient='records')
        else:
            clientes_sin_ningun_cambio = []

        return clientes_sin_ningun_cambio
    
    def obtener_excepciones_datos_unicos(self):
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_clientes,df_obtener_clientes_existentes = self.obtener_data_serializada()

            excepciones = df_clientes[
                          df_clientes.apply(lambda x: (
                            (x['razon_social'] in set(df_obtener_clientes_existentes['razon_social'])) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['razon_social'] == x['razon_social'], 'cliente_id_erp'].values[0]) 
                            or
                            (x['identificacion'] in set(df_obtener_clientes_existentes['identificacion'])) and
                            (x['cliente_id_erp'] != df_obtener_clientes_existentes.loc[df_obtener_clientes_existentes['identificacion'] == x['identificacion'], 'cliente_id_erp'].values[0])
                        ), axis=1)
                    ]
            
            resultado_actualizacion = excepciones.to_dict(orient='records')
      
        else:
            resultado_actualizacion = []
            
        return resultado_actualizacion
    
    def insertar_informacion(self, novedades_unidad_organizativa: List):
        if len(novedades_unidad_organizativa) > 0:
            informacion_unidad_gerencia = self.procesar_datos_minuscula(novedades_unidad_organizativa)
            session.bulk_insert_mappings(ProyectoCliente, informacion_unidad_gerencia)
            return informacion_unidad_gerencia
        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        if len(actualizacion_gerencia_unidad_organizativa) > 0:
            informacion_unidad_gerencia = self.procesar_datos_minuscula(actualizacion_gerencia_unidad_organizativa)
            session.bulk_update_mappings(ProyectoCliente, informacion_unidad_gerencia)
            return informacion_unidad_gerencia
        return "No se han actualizado datos"
    
    def procesar_datos_minuscula(self,datos):
        df = pd.DataFrame(datos)
        df[['cliente_id_erp', 'razon_social']] = df[['cliente_id_erp', 'razon_social']].apply(lambda x: x.str.lower())
        return df.to_dict(orient='records')
    

    def obtener_data_serializada(self):
        
            df_clientes = pd.DataFrame(self.__data_usuario_cliente)
            df_obtener_clientes_existentes = pd.DataFrame(self.__obtener_clientes_existente)
            
            df_clientes["cliente_id_erp"] = df_clientes["cliente_id_erp"].str.lower()
            df_obtener_clientes_existentes["cliente_id_erp"] = df_obtener_clientes_existentes["cliente_id_erp"].str.lower()
            
            df_clientes["razon_social"] = df_clientes["razon_social"].str.lower()
            df_obtener_clientes_existentes["razon_social"] = df_obtener_clientes_existentes["razon_social"].str.lower()
            
            return df_clientes,df_obtener_clientes_existentes