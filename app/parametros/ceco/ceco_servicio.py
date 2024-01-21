import pandas as pd
from fastapi import  UploadFile
from app.parametros.ceco.model.ceco_model import ProyectoCeco
from app.database.db import session
import numpy as np
from sqlalchemy.exc import SQLAlchemyError

class Ceco:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        self.__obtener_cecos_existente = self.obtener()
        estructuracion_datos_usuario_ceco = self.__proceso_de_informacion_estructuracion()
        self.__data_usuario_ceco = estructuracion_datos_usuario_ceco['resultado']
        self.__duplicados = estructuracion_datos_usuario_ceco['duplicados']
    
        
    def validacion_existe_informacion(self)->bool:
        return len(self.__data_usuario_ceco) > 0 or len(self.__obtener_cecos_existente) > 0
    
    def proceso_sacar_estado(self):
            
            cecos_registros = self.ceco_nuevos()['estado']
            cecos_actualizacion =self.actualizar_ceco_filtro()['estado']
            cecos_sin_cambios = self.obtener_no_sufrieron_cambios()['estado']
            cecos_excepciones = self.obtener_excepciones_datos_unicos()['estado']

            # Crear un conjunto con todos los valores de estado
            estados = {cecos_registros, cecos_actualizacion, cecos_sin_cambios, cecos_excepciones}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados
        
    def transacciones(self):
        try:
            if self.validacion_existe_informacion():
                registro_de_cecos = self.ceco_nuevos()['respuesta']
                actualizacion_cecos = self.actualizar_ceco_filtro()['respuesta']
                
                log_transaccion_registro = self.insertar_informacion(registro_de_cecos)
                log_transaccion_actualizar = self.actualizar_informacion(actualizacion_cecos)
        
                estado_id = self.proceso_sacar_estado()
                log_transaccion_registro_gerencia = {
                        "log_transaccion_excel": {
                            'Respuesta':[
                                {
                                    "gerencia_registradas": log_transaccion_registro,
                                    "gerencias_actualizadas": log_transaccion_actualizar,
                                    'cecos_sin_cambios':self.obtener_no_sufrieron_cambios()['respuesta'],
                         
                                }
                            ],
                            'errores':{
                                'cecos_excepciones':self.obtener_excepciones_datos_unicos()['respuesta'],
                                'duplicados':self.__duplicados
                            }
                
                        },
                        'estado':{
                            'id':estado_id
                        }
                    }
                
                return log_transaccion_registro_gerencia
            
            return  { 'Mensaje':'No hay informacion para realizar el proceso',
                    'duplicados':self.__duplicados,'estado':3}                
        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()
    
    def __proceso_de_informacion_estructuracion(self):
        df = pd.read_excel(self.__file.file)
        
        df.columns = df.columns.str.strip()
        # Imprimir las columnas reales del DataFrame
        selected_columns = ["ID proyecto", "Nombre del proyecto"]

        selected_data = df[selected_columns]
          # Cambiar los Nombres de las columnas
        selected_data = selected_data.rename(
            columns={
               "ID proyecto": "ceco_id_erp", 
                "Nombre del proyecto": "nombre"
            }
        )
        
        selected_data["ceco_id_erp"] = selected_data["ceco_id_erp"]
        selected_data["nombre"] = selected_data["nombre"]
        
        # uso de eliminacion de espacios en blancos
        df_filtered = selected_data.dropna()
        
        duplicados_ceco_erp = df_filtered.duplicated(subset='ceco_id_erp', keep=False)
        duplicados_ceco = df_filtered.duplicated(subset='nombre', keep=False)
        # Filtrar DataFrame original
        resultado = df_filtered[~(duplicados_ceco_erp | duplicados_ceco)].to_dict(orient='records')
        duplicados = df_filtered[(duplicados_ceco_erp | duplicados_ceco)].to_dict(orient='records')
        
        return {'resultado':resultado,'duplicados':duplicados}
    
    
    def obtener(self):
        informacion_ceco = session.query(ProyectoCeco).all()
        # Convertir lista de objetos a lista de diccionarios
        ceco = [ceco.to_dict() for ceco in informacion_ceco]
        return ceco
    
    def ceco_nuevos(self):
        validacion = self.validacion_existe_informacion()
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente)
                
            df_ceco["ceco_id_erp"] = df_ceco["ceco_id_erp"]
            df_ceco["nombre"] = df_ceco["nombre"]

            df_obtener_ceco_existentes["ceco_id_erp"] = df_obtener_ceco_existentes["ceco_id_erp"]
            df_obtener_ceco_existentes["nombre"] = df_obtener_ceco_existentes["nombre"]
            
            resultado = df_ceco[
                        ~df_ceco.apply(lambda x: 
                            ((x['ceco_id_erp'].lower() in set(df_obtener_ceco_existentes['ceco_id_erp'].str.lower())) 
                            or 
                            (x['nombre'].lower() in set(df_obtener_ceco_existentes['nombre'].str.lower()))
                            ), axis=1)
                    ]
  
            registro_cecos = resultado.to_dict(orient='records')
        else:
            registro_cecos = []
            
        return  {'respuesta':registro_cecos,'estado':1} if len(registro_cecos) > 0 else {'respuesta':registro_cecos,'estado':0}
    
    def obtener_no_sufrieron_cambios(self):
        validacion = self.validacion_existe_informacion()
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente)
            
            df_ceco["ceco_id_erp"] = df_ceco["ceco_id_erp"]
            
            df_obtener_ceco_existentes["ceco_id_erp"] = df_obtener_ceco_existentes["ceco_id_erp"]
            
            df_ceco["nombre"] = df_ceco["nombre"]
            
            df_obtener_ceco_existentes["nombre"] = df_obtener_ceco_existentes["nombre"]
            
            resultado = pd.merge(df_ceco, df_obtener_ceco_existentes, how='inner', on = ['ceco_id_erp', 'nombre'])
            # resultado = df_ceco[df_ceco.isin(df_obtener_ceco_existentes.to_dict('list')).all(axis=1)]
            if resultado.empty:
                return {'respuesta':[],'estado':0}
            
            cecos_sin_cambios = resultado.to_dict(orient='records')
        else:
            cecos_sin_cambios = []

        return {'respuesta':cecos_sin_cambios,'estado':3} if len(cecos_sin_cambios) > 0 else {'respuesta':cecos_sin_cambios,'estado':0}
    
    def actualizar_ceco_filtro(self):
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente)
            
            df_ceco["ceco_id_erp"] = df_ceco["ceco_id_erp"]
            df_obtener_ceco_existentes["ceco_id_erp"] = df_obtener_ceco_existentes["ceco_id_erp"]
            
            df_ceco["nombre"] = df_ceco["nombre"]
            df_obtener_ceco_existentes["nombre"] = df_obtener_ceco_existentes["nombre"]
            
            resultado = pd.merge(
                df_ceco[['ceco_id_erp','nombre']],
                df_obtener_ceco_existentes[['id','ceco_id_erp','nombre']],
                left_on=['ceco_id_erp'],
                right_on=['ceco_id_erp'],
                how='inner',
            )
        
            # Seleccionar las columnas deseadas
            resultado_final = resultado[['id', 'ceco_id_erp', 'nombre_x']].rename(columns={'nombre_x':'nombre'})
            
            resultado = resultado_final[
                         ~resultado_final.apply(lambda x: 
                        ((x['nombre'].lower() in set(df_obtener_ceco_existentes['nombre'].str.lower())) and
                        (x['ceco_id_erp'] != df_obtener_ceco_existentes.loc[df_obtener_ceco_existentes['nombre'].str.lower() == x['nombre'].lower(), 'ceco_id_erp'].values[0])
                        ), axis=1)
                ]

            if resultado.empty:
                return {'respuesta':[],'estado':0}
            
            resultado_actualizacion = resultado.to_dict(orient='records')

            ceco_filtro = self.obtener_no_sufrieron_cambios()['respuesta']
            
            if len(ceco_filtro) != 0:
                df_ceco = pd.DataFrame(ceco_filtro)
                df_filtrado = resultado[~resultado[['nombre']].isin(df_ceco[['nombre']].to_dict('list')).all(axis=1)]
                filtro_ceco_actualizacion = df_filtrado.to_dict(orient = 'records')
                return {'respuesta':filtro_ceco_actualizacion,'estado':2} if len(filtro_ceco_actualizacion) > 0 else {'respuesta':filtro_ceco_actualizacion,'estado':0}
            
        else:
            resultado_actualizacion = []
            
        return {'respuesta':resultado_actualizacion,'estado':2} if len(resultado_actualizacion) > 0 else {'respuesta':resultado_actualizacion,'estado':0}
    
    # obtengo las excepciones del usuario me esta enviando informacion que debe ser unica y la filtro lo que me viene en la bd contra lo que me envia el usuario
    # y le devuelvo la informacion
    def obtener_excepciones_datos_unicos(self):
        
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente)
            
            df_ceco["ceco_id_erp"] = df_ceco["ceco_id_erp"]
            df_obtener_ceco_existentes["ceco_id_erp"] = df_obtener_ceco_existentes["ceco_id_erp"]
            
            df_ceco["nombre"] = df_ceco["nombre"]
            df_obtener_ceco_existentes["nombre"] = df_obtener_ceco_existentes["nombre"]
            
            
            resultado = df_ceco[
                        df_ceco.apply(lambda x: 
                            (
                                (x['nombre'].lower() in df_obtener_ceco_existentes['nombre'].str.lower().values) and
                                (x['ceco_id_erp'].lower() != df_obtener_ceco_existentes.loc[df_obtener_ceco_existentes['nombre'].str.lower() == x['nombre'].lower(), 'ceco_id_erp'].str.lower().values[0])
                            ),
                            axis=1
                        ) |
                        df_ceco['ceco_id_erp'].str.lower().isin(df_obtener_ceco_existentes['ceco_id_erp'].str.lower().values)
                    ]
            
            if resultado.empty:
                return {'respuesta':[],'estado':0}
            
            actualizar_ = self.actualizar_ceco_filtro()['respuesta']
            
            if len(actualizar_) > 0:
                df_ceco_filtro = pd.DataFrame(actualizar_)
                df_filtrado = resultado[~resultado[['nombre','ceco_id_erp']].isin(df_ceco_filtro[['nombre','ceco_id_erp']].to_dict('list')).all(axis=1)]
                filtro_ceco =  df_filtrado.to_dict(orient='records')
                return {'respuesta':filtro_ceco,'estado':3} if len(filtro_ceco) > 0 else {'respuesta':filtro_ceco,'estado':0}
            
                        
            obtener_excepciones = resultado.to_dict(orient='records')
            ceco_filtro = self.obtener_no_sufrieron_cambios()['respuesta']
            
            if len(ceco_filtro) != 0:
                df_ceco = pd.DataFrame(ceco_filtro)
                df_filtrado = resultado[~resultado[['nombre','ceco_id_erp']].isin(df_ceco[['nombre','ceco_id_erp']].to_dict('list')).all(axis=1)]
                filtro_ceco_actualizacion =  df_filtrado.to_dict(orient='records')
                return {'respuesta':filtro_ceco_actualizacion,'estado':3} if len(filtro_ceco_actualizacion) > 0 else {'respuesta':filtro_ceco_actualizacion,'estado':0}
      
        else:
            obtener_excepciones = []
            
        return  {'respuesta':obtener_excepciones,'estado':4} if len(obtener_excepciones) > 0 else {'respuesta':obtener_excepciones,'estado':0}
    
    def insertar_informacion(self, novedades_unidad_organizativa):
        if len(novedades_unidad_organizativa) > 0:
            session.bulk_insert_mappings(ProyectoCeco, novedades_unidad_organizativa)
            session.commit()
            return novedades_unidad_organizativa

        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        if len(actualizacion_gerencia_unidad_organizativa) > 0:
            session.bulk_update_mappings(ProyectoCeco, actualizacion_gerencia_unidad_organizativa)
            session.commit()
            return actualizacion_gerencia_unidad_organizativa

        return "No se han actualizado datos"
    
    
    # def procesar_datos_minuscula(self,datos):
    #     df = pd.DataFrame(datos)
    #     df[['ceco_id_erp', 'nombre']] = df[['ceco_id_erp', 'nombre']].apply(lambda x: x.str.lower())
    #     return df.to_dict(orient='records')