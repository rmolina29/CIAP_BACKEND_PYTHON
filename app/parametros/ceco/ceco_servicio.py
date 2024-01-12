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
        
    def transacciones(self):
        try:
            registro_de_cecos = self.ceco_nuevos()
            actualizacion_cecos = self.actualizar_ceco_filtro()
            
            log_transaccion_registro = self.insertar_informacion(registro_de_cecos)
            log_transaccion_actualizar = self.actualizar_informacion(actualizacion_cecos)
            log_transaccion_registro_gerencia = {
                    "log_transaccion_excel": {
                    #    'sin_cambios':self.obtener_no_sufrieron_cambios(),
                    #    'registo':self.insertar_informacion(registro_clientes),
                    #    'actualizar':self.actualizar_informacion(actualizar_clientes),
                    'cecos_excepciones':self.obtener_excepciond_datos_unicos(),
                    'ceco_registros':log_transaccion_registro,
                    'cecos_actualizacion':log_transaccion_actualizar,
                    'cecos_sin_cambios':self.obtener_no_sufrieron_cambios(),
                    'duplicados':self.__duplicados
                    }
            }
            
            return log_transaccion_registro_gerencia
        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()
    
    def __proceso_de_informacion_estructuracion(self):
        df = pd.read_excel(self.__file.file)
        # Imprimir las columnas reales del DataFrame
        selected_columns = ["ID proyecto (ERP)", "Nombre", "Descripcion"]

        selected_data = df[selected_columns]
        
          # Cambiar los Nombres de las columnas
        selected_data = selected_data.rename(
            columns={
               "ID proyecto (ERP)": "ceco_id_erp", 
                "Nombre": "nombre", 
                "Descripcion":'descripcion'
            }
        )
        
        selected_data["ceco_id_erp"] = selected_data["ceco_id_erp"].str.lower()
        selected_data["nombre"] = selected_data["nombre"].str.lower()
        
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
                
            df_ceco["ceco_id_erp"] = df_ceco["ceco_id_erp"].str.lower()
            df_ceco["nombre"] = df_ceco["nombre"].str.lower()

            df_obtener_ceco_existentes["ceco_id_erp"] = df_obtener_ceco_existentes["ceco_id_erp"].str.lower()
            df_obtener_ceco_existentes["nombre"] = df_obtener_ceco_existentes["nombre"].str.lower()
                    
            resultado = df_ceco[
                        ~df_ceco.apply(lambda x: 
                            ((x['ceco_id_erp'] in set(df_obtener_ceco_existentes['ceco_id_erp'])) 
                            or 
                            (x['nombre'] in set(df_obtener_ceco_existentes['nombre']))
                            ), axis=1)
                    ]
  
            registro_cecos = resultado.to_dict(orient='records')
        else:
            registro_cecos = []
            
        return registro_cecos
    
    def obtener_no_sufrieron_cambios(self):
        validacion = self.validacion_existe_informacion()
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente)
            
            df_ceco["ceco_id_erp"] = df_ceco["ceco_id_erp"].str.lower()
            df_obtener_ceco_existentes["ceco_id_erp"] = df_obtener_ceco_existentes["ceco_id_erp"].str.lower()
            
            df_ceco["nombre"] = df_ceco["nombre"].str.lower()
            df_obtener_ceco_existentes["nombre"] = df_obtener_ceco_existentes["nombre"].str.lower()
            
            resultado = pd.merge(df_ceco, df_obtener_ceco_existentes, how='inner', on=['ceco_id_erp', 'nombre','descripcion'])
            
            cecos_sin_cambios = resultado.to_dict(orient='records')
        else:
            cecos_sin_cambios = []

        return cecos_sin_cambios
    
    def actualizar_ceco_filtro(self):
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente)
            
            df_ceco["ceco_id_erp"] = df_ceco["ceco_id_erp"].str.lower()
            df_obtener_ceco_existentes["ceco_id_erp"] = df_obtener_ceco_existentes["ceco_id_erp"].str.lower()
            
            df_ceco["nombre"] = df_ceco["nombre"].str.lower()
            df_obtener_ceco_existentes["nombre"] = df_obtener_ceco_existentes["nombre"].str.lower()
            
            resultado = pd.merge(
                df_ceco[['ceco_id_erp','nombre','descripcion']],
                df_obtener_ceco_existentes[['id','ceco_id_erp','nombre','descripcion']],
                left_on=['ceco_id_erp'],
                right_on=['ceco_id_erp'],
                how='inner',
            )
        
            # Seleccionar las columnas deseadas
            resultado_final = resultado[['id', 'ceco_id_erp', 'nombre_x','descripcion_x']].rename(columns={'nombre_x':'nombre','descripcion_x':'descripcion'})
            
            resultado = resultado_final[
                         ~resultado_final.apply(lambda x: 
                        ((x['nombre'] in set(df_obtener_ceco_existentes['nombre'])) and
                        (x['ceco_id_erp'] != df_obtener_ceco_existentes.loc[df_obtener_ceco_existentes['nombre'] == x['nombre'], 'ceco_id_erp'].values[0])
                        ), axis=1)
                ]
            
            resultado_actualizacion = resultado.to_dict(orient='records')

            ceco_filtro = self.obtener_no_sufrieron_cambios()
            
            if len(ceco_filtro) != 0:
                df_ceco = pd.DataFrame(ceco_filtro)
                df_filtrado = resultado[~resultado.isin(df_ceco.to_dict('list')).all(axis=1)]
                return df_filtrado.to_dict(orient='records')
            
            return resultado_actualizacion
        else:
            resultado_actualizacion = []
            
        return resultado_actualizacion
    
    # obtengo las excepciones del usuario me esta enviando informacion que debe ser unica y la filtro lo que me viene en la bd contra lo que me envia el usuario
    # y le devuelvo la informacion
    def obtener_excepciond_datos_unicos(self):
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente)
            
            df_ceco["ceco_id_erp"] = df_ceco["ceco_id_erp"].str.lower()
            df_obtener_ceco_existentes["ceco_id_erp"] = df_obtener_ceco_existentes["ceco_id_erp"].str.lower()
            
            df_ceco["nombre"] = df_ceco["nombre"].str.lower()
            df_obtener_ceco_existentes["nombre"] = df_obtener_ceco_existentes["nombre"].str.lower()
            
            
            resultado = df_ceco[
                        df_ceco.apply(lambda x: 
                            
                            ((x['nombre'] in set(df_obtener_ceco_existentes['nombre'])) and
                             (x['ceco_id_erp'] != df_obtener_ceco_existentes.loc[df_obtener_ceco_existentes['nombre'] == x['nombre'], 'ceco_id_erp'].values[0])
                            ), axis=1)
                    ]
            
            resultado_actualizacion = resultado.to_dict(orient='records')
      
        else:
            resultado_actualizacion = []
            
        return resultado_actualizacion
    
    def insertar_informacion(self, novedades_unidad_organizativa):
        if len(novedades_unidad_organizativa) > 0:
            informacion_unidad_gerencia = self.procesar_datos_minuscula(novedades_unidad_organizativa)
            session.bulk_insert_mappings(ProyectoCeco, informacion_unidad_gerencia)
            return informacion_unidad_gerencia

        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        if len(actualizacion_gerencia_unidad_organizativa) > 0:
            informacion_unidad_gerencia = self.procesar_datos_minuscula(actualizacion_gerencia_unidad_organizativa)
            session.bulk_update_mappings(ProyectoCeco, informacion_unidad_gerencia)
            return informacion_unidad_gerencia

        return "No se han actualizado datos"
    
    
    def procesar_datos_minuscula(self,datos):
        df = pd.DataFrame(datos)
        df[['ceco_id_erp', 'nombre']] = df[['ceco_id_erp', 'nombre']].apply(lambda x: x.str.lower())
        return df.to_dict(orient='records')