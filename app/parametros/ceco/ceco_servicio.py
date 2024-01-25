import pandas as pd
from fastapi import  UploadFile
from app.parametros.ceco.model.ceco_model import ProyectoCeco
from app.database.db import session
from sqlalchemy.exc import SQLAlchemyError
from app.funcionalidades_archivos.funciones_archivos_excel import GestorExcel
from app.parametros.mensajes_resultado.mensajes import CecoMensaje, GlobalMensaje

class Ceco:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        self.__obtener_cecos_existente = self.obtener()
        self.__obtener_cecos_existente_estado_activo = self.obtener_ceco_estado()
        estructuracion_datos_usuario_ceco = self.__proceso_de_informacion_estructuracion()
        self.__data_usuario_ceco = estructuracion_datos_usuario_ceco['resultado']
        self.__duplicados = estructuracion_datos_usuario_ceco
    
        
    def validacion_existe_informacion(self,obtener_ceco_existentes)->bool:
        return len(self.__data_usuario_ceco) > 0 and len(obtener_ceco_existentes) > 0
    
    def proceso_sacar_estado(self):
            
            cecos_registros = self.ceco_nuevos()['estado']
            cecos_actualizacion =self.actualizar_ceco_filtro()['estado']
            cecos_sin_cambios = self.obtener_no_sufrieron_cambios()['estado']
            cecos_excepciones = self.obtener_excepciones_datos_unicos()['estado']

            # Crear un conjunto con todos los valores de estado
            estados = {cecos_registros, cecos_actualizacion, cecos_sin_cambios, cecos_excepciones}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados if len(estados_filtrados) > 0 else 0  
        
    def transacciones(self):
        try:
            if self.validacion_existe_informacion(self.__obtener_cecos_existente_estado_activo):
                registro_de_cecos = self.ceco_nuevos()['respuesta']
                actualizacion_cecos = self.actualizar_ceco_filtro()['respuesta']
                log_transaccion_registro = self.insertar_informacion(registro_de_cecos)
                log_transaccion_actualizar = self.actualizar_informacion(actualizacion_cecos)
                excepcion_ceco_existe = self.obtener_excepciones_datos_unicos()['respuesta']
                    
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
                                'cecos_excepciones':{'datos':excepcion_ceco_existe,'mensaje':CecoMensaje.EXCEPCION_CECO_UNICO.value} if len(excepcion_ceco_existe) > 0 else [],
                                'duplicados':{'datos':self.__duplicados['duplicados'],'mensaje':GlobalMensaje.mensaje(self.__duplicados['cantidad_duplicados'])} if len(self.__duplicados['duplicados']) else []
                            }
                
                        },
                        'estado':{
                            'id':estado_id
                        }
                    }
                
                return log_transaccion_registro_gerencia
            return  {   
                        'Mensaje': GlobalMensaje.NO_HAY_INFORMACION.value,
                        'duplicados':self.__duplicados['duplicados'],
                        'duplicados': {'datos':self.__duplicados['duplicados'],'mensaje':GlobalMensaje.mensaje(self.__duplicados['cantidad_duplicados'])} if len(self.__duplicados['duplicados']) else [],
                        'estado': self.__duplicados['estado'] if 'estado' in self.__duplicados and isinstance(self.__duplicados['estado'], list) else [self.__duplicados.get('estado', self.__duplicados['estado'])]
                    }                
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

        df_excel = df[selected_columns]
        
        if df_excel.empty:
                return {'resultado': [], 'duplicados': [],'estado':0}
          # Cambiar los Nombres de las columnas
        df_excel = df_excel.rename(
            columns={
               "ID proyecto": "ceco_id_erp", 
                "Nombre del proyecto": "nombre"
            }
        )
        
        df_excel["ceco_id_erp"] = df_excel["ceco_id_erp"].str.strip()
        df_excel["nombre"] = df_excel["nombre"].str.strip()
        
        # uso de eliminacion de espacios en blancos
        df_filtered = df_excel.dropna()
        
        duplicados_ceco_erp = df_filtered.duplicated(subset='ceco_id_erp', keep=False)
        duplicados_ceco = df_filtered.duplicated(subset='nombre', keep=False)
        # Filtrar DataFrame original
        datos_excel_ceco = df_filtered[~(duplicados_ceco_erp | duplicados_ceco)].to_dict(orient='records')
        
        duplicados = df_filtered[(duplicados_ceco_erp | duplicados_ceco)].to_dict(orient='records')

        cantidad_duplicados = len(duplicados)

        return {    
                
                'resultado':datos_excel_ceco,
                'duplicados':duplicados[0] if cantidad_duplicados > 0 else [] , 
                'cantidad_duplicados':cantidad_duplicados,
                'estado':3 if cantidad_duplicados > 0 else 0
                
                }
    
    
    def obtener(self):
        informacion_ceco = session.query(ProyectoCeco).all()
        # Convertir lista de objetos a lista de diccionarios
        ceco = [ceco.to_dict() for ceco in informacion_ceco]
        return ceco
    
    def obtener_ceco_estado(self,estado = 1):
        informacion_ceco = session.query(ProyectoCeco).filter_by(estado = estado).all()
        # Convertir lista de objetos a lista de diccionarios
        ceco = [ceco.to_dict() for ceco in informacion_ceco]
        return ceco
    
    def ceco_nuevos(self):
        validacion = self.validacion_existe_informacion(self.__obtener_cecos_existente)
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
        validacion = self.validacion_existe_informacion(self.__obtener_cecos_existente_estado_activo)
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente_estado_activo)
            
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
        validacion = self.validacion_existe_informacion(self.__obtener_cecos_existente_estado_activo)
        
        if validacion:
            df_ceco = pd.DataFrame(self.__data_usuario_ceco)
            df_obtener_ceco_existentes = pd.DataFrame(self.__obtener_cecos_existente_estado_activo)
            
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
        
        validacion = self.validacion_existe_informacion(self.__obtener_cecos_existente)
        
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
            ceco_filtro = self.obtener_no_sufrieron_cambios()['respuesta']
            columnas_ceco_filtro = ['nombre','ceco_id_erp']
            
            gestor_proceso_excel = GestorExcel(columnas_ceco_filtro)
            
            filtrar_las_actualizaciones = gestor_proceso_excel.filtro_de_excpeciones(ceco_filtro,actualizar_,resultado)
            
            if len(filtrar_las_actualizaciones['respuesta']) > 0 or filtrar_las_actualizaciones['estado']:
                return {'respuesta': filtrar_las_actualizaciones['respuesta'], 'estado' : 3 if len(filtrar_las_actualizaciones['respuesta'])>0 else 0}
      
            obtener_excepciones = resultado.to_dict(orient='records')
        else:
            obtener_excepciones = []
            
        return  {'respuesta':obtener_excepciones,'estado':3} if len(obtener_excepciones) > 0 else {'respuesta':obtener_excepciones,'estado':0}
    
    def insertar_informacion(self, novedades_unidad_organizativa):
        cantidad_de_registros = len(novedades_unidad_organizativa)
        if len(novedades_unidad_organizativa) > 0:
            # session.bulk_insert_mappings(ProyectoCeco, novedades_unidad_organizativa)
            # session.commit()
            return  {'mensaje': f'Se han realizado {cantidad_de_registros} registros exitosos.' if cantidad_de_registros > 1 else  f'Se ha registrado un ({cantidad_de_registros}) proyecto ceco exitosamente.'}
        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        cantidad_de_actualizaciones = len(actualizacion_gerencia_unidad_organizativa)
        if cantidad_de_actualizaciones > 0:
            
            # session.bulk_update_mappings(ProyectoCeco, actualizacion_gerencia_unidad_organizativa)
            # session.commit()
            return  {'mensaje': f'Se han realizado {cantidad_de_actualizaciones} actualizaciones exitosos.' if cantidad_de_actualizaciones > 1 else  f'Se ha actualizado un ({cantidad_de_actualizaciones}) registro exitosamente.'}
        return "No se han actualizado datos"
    