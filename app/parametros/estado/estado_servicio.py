
from fastapi import  UploadFile
import pandas as pd
from app.database.db import session
from app.funcionalidades_archivos.funciones_archivos_excel import GestorExcel
from app.parametros.estado.model.estado_model import ProyectoEstado
from app.parametros.mensajes_resultado.mensajes import CecoMensaje, EstadoMensaje, GlobalMensaje


class Estado:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        # data para la comparacion de informacion sacandola de la base de datos
        self.__obtener_estados_existente = self.obtener()
        self.__obtener_estados_existente_activos = self.obtener_por_estado()
        estados_usuario_excel = self.__proceso_de_informacion_estructuracion()
        self.__estados = estados_usuario_excel['resultado']
        self.__estados_duplicados = estados_usuario_excel
        self.gestor_excel = GestorExcel()
        
    
    def proceso_sacar_estado(self):
            
            registro_estados = self.estados_nuevos()['estado']
            actualizacion_estados = self.filtro_estado_actualizar()['estado']
            estado_sin_cambios = self.obtener_no_sufrieron_cambios()['estado']
            estado_exepciones = self.obtener_excepciones_datos_unicos_estado()['estado']
            # estado_excepciones = self.obtener_excepciones_datos_unicos()['estado']

            # Crear un conjunto con todos los valores de estado
            estados = {registro_estados, actualizacion_estados, estado_sin_cambios,estado_exepciones}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados
    
    def transacciones(self):
        if self.validacion_existe_informacion(self.__obtener_estados_existente):
            registro_estados = self.estados_nuevos()['respuesta']
            actualizacion_estados = self.filtro_estado_actualizar()['respuesta']
            estado_id = self.proceso_sacar_estado()
            excpecion = self.obtener_excepciones_datos_unicos_estado()['respuesta']
            
            log_transaccion_registro_estado = {
                        "log_transaccion_excel": {
                            'Respuesta':[
                                {
                                    'registrar':self.insertar_informacion(registro_estados),
                                    'actualizacion':self.actualizar_informacion(actualizacion_estados),
                                    'sin_cambios':self.obtener_no_sufrieron_cambios()['respuesta']
                                }
                            ],
                            'errores':{
                                'duplicados':{'datos':self.__estados_duplicados['duplicados'],'mensaje':GlobalMensaje.mensaje(self.__estados_duplicados['cantidad_duplicados'])} if len(self.__estados_duplicados['duplicados']) else [],
                                'excpeciones':{'datos':excpecion,'mensaje':EstadoMensaje.EXCEPCION_ESTADO_UNICO.value} if len(excpecion) > 0 else []
                            }
                
                        },
                        'estado':{
                            'id':estado_id
                        }
                    }
            
            return log_transaccion_registro_estado
        
        
        return { 
                    'Mensaje':GlobalMensaje.NO_HAY_INFORMACION.value,
                    'duplicados' : {'datos':self.__estados_duplicados['duplicados'],'mensaje':GlobalMensaje.mensaje(self.__estados_duplicados['cantidad_duplicados'])} if len(self.__estados_duplicados['duplicados']) else [],
                    'estado': self.gestor_excel.transformacion_estados(self.__estados_duplicados)
                }  

            
   
    
    def __proceso_de_informacion_estructuracion(self):
        df = pd.read_excel(self.__file.file)
        df.columns = df.columns.str.strip()
        # Imprimir las columnas reales del DataFrame
        selected_columns = ["ID Estado (ERP)", "Estado"]

        df_excel = df[selected_columns]
        
        if df_excel.empty:
            return {'resultado': [], 'duplicados': [],'cantidad_duplicados':0,'estado':0}
          # Cambiar los nombres de las columnas
        df_excel = df_excel.rename(
            columns={
               "ID Estado (ERP)": "estado_id_erp", 
                "Estado": "descripcion" 
            }
        )
        
        df_excel["estado_id_erp"] = df_excel["estado_id_erp"].str.strip()
        df_excel["descripcion"] = df_excel["descripcion"].str.strip()
        
        df_filtered = df_excel.dropna()
        
        estado_duplicado = df_filtered.duplicated(subset = 'estado_id_erp', keep=False)
        duplicado_descripcion = df_filtered.duplicated(subset = 'descripcion', keep=False)
        # Filtrar DataFrame original
        resultado = df_filtered[~(estado_duplicado | duplicado_descripcion)].to_dict(orient='records')
        duplicados = df_filtered[(estado_duplicado | duplicado_descripcion)].to_dict(orient='records')
        
        cantidad_duplicados = len(duplicados)
        
        return {
                'resultado':resultado,
                'duplicados':duplicados[0] if cantidad_duplicados > 0 else [] ,
                'cantidad_duplicados':cantidad_duplicados,
                'estado':3 if cantidad_duplicados > 0 else 0
                }
    
    def obtener(self):
        estados = session.query(ProyectoEstado).all()
        # Convertir lista de objetos a lista de diccionarios
        estado = [estado.to_dict() for estado in estados ]
        return estado
    
    def obtener_por_estado(self):
        estados_activos = session.query(ProyectoEstado).filter_by(estado = 1).all()
        # Convertir lista de objetos a lista de diccionarios
        estado = [estado.to_dict() for estado in estados_activos ]
        return estado
    
    
    def validacion_existe_informacion(self,estado_existentes)->bool:
        return len(self.__estados) > 0 and len(estado_existentes)>0
    
    def obtener_excepciones_datos_unicos_estado(self):
        
        validacion = self.validacion_existe_informacion(self.__obtener_estados_existente)
        
        if validacion:
            df_estado = pd.DataFrame(self.__estados)
            df_obtener_estado_existentes = pd.DataFrame(self.__obtener_estados_existente)

            
            resultado = df_estado[
                        df_estado.apply(lambda x: 
                            (
                                (x['descripcion'].lower() in df_obtener_estado_existentes['descripcion'].str.lower().values) and
                                (x['estado_id_erp'].lower() != df_obtener_estado_existentes.loc[df_obtener_estado_existentes['descripcion'].str.lower() == x['descripcion'].lower(), 'estado_id_erp'].str.lower().values[0])
                            ),
                            axis=1
                        ) |
                        df_estado['estado_id_erp'].str.lower().isin(df_obtener_estado_existentes['estado_id_erp'].str.lower().values)
                    ]
            
            if resultado.empty:
                return {'respuesta':[],'estado':0}
              
            
            filtro_actualizacion = self.filtro_estado_actualizar()['respuesta']
            estado_filtro_no_sufrieron_cambios = self.obtener_no_sufrieron_cambios()['respuesta']
            
            columnas_ceco_filtro = ['descripcion','estado_id_erp']
            
            gestor_proceso_excel = GestorExcel(columnas_ceco_filtro)
            
            filtro_de_excepciones_y_actualizaciones = gestor_proceso_excel.filtro_de_excpeciones(estado_filtro_no_sufrieron_cambios,filtro_actualizacion,resultado)
            if len(filtro_de_excepciones_y_actualizaciones['respuesta']) > 0 or filtro_de_excepciones_y_actualizaciones['estado'] == 3:
                return  {'respuesta': filtro_de_excepciones_y_actualizaciones['respuesta'],'estado': filtro_de_excepciones_y_actualizaciones['estado']}
            
            obtener_excepciones_estado =  resultado.to_dict(orient='records')
        else:
            obtener_excepciones_estado = []
        return  {'respuesta':obtener_excepciones_estado,'estado':3} if len(obtener_excepciones_estado) > 0 else {'respuesta':obtener_excepciones_estado,'estado':0}
    
    def estados_nuevos(self):
        
        validacion = self.validacion_existe_informacion(self.__obtener_estados_existente)
        
        if validacion:
            df_estado = pd.DataFrame(self.__estados)
            df_obtener_estado_existentes = pd.DataFrame(self.__obtener_estados_existente)
            
            df_estado["estado_id_erp"] = df_estado["estado_id_erp"]
            df_estado["descripcion"] = df_estado["descripcion"]

            df_obtener_estado_existentes["estado_id_erp"] = df_obtener_estado_existentes["estado_id_erp"]
            df_obtener_estado_existentes["descripcion"] = df_obtener_estado_existentes["descripcion"]
                
            resultado = df_estado[
                    ~df_estado.apply(lambda x: 
                        ((x['estado_id_erp'].lower()  in set(df_obtener_estado_existentes['estado_id_erp'].str.lower() )) 
                         or 
                         (x['descripcion'].lower() in set(df_obtener_estado_existentes['descripcion'].str.lower() ))
                         ), axis=1)
                ]
                                
            # Resultado final
            estado_registro = resultado.to_dict(orient='records')
            
        else:
            estado_registro = []
            
        return {'respuesta':estado_registro,'estado':1} if len(estado_registro)>0 else {'respuesta':estado_registro,'estado':0}
    
    
    def filtro_estado_actualizar(self):
        
        validacion = self.validacion_existe_informacion(self.__obtener_estados_existente_activos)
        
        if validacion:
            df_estado = pd.DataFrame(self.__estados)
            df_obtener_estados_existentes = pd.DataFrame(self.__obtener_estados_existente_activos)
            
            df_estado["estado_id_erp"] = df_estado["estado_id_erp"]
            df_obtener_estados_existentes["estado_id_erp"] = df_obtener_estados_existentes["estado_id_erp"]
            
            df_estado["descripcion"] = df_estado["descripcion"]
            df_obtener_estados_existentes["descripcion"] = df_obtener_estados_existentes["descripcion"]
            
            resultado = pd.merge(
                df_estado[['estado_id_erp','descripcion']],
                df_obtener_estados_existentes[['id','estado_id_erp','descripcion']],
                left_on=['estado_id_erp'],
                right_on=['estado_id_erp'],
                how='inner',
            )
            
            resultado_filtrado = resultado[
                ((resultado['descripcion_x'] != resultado['descripcion_y']))
            ]

            # Seleccionar las columnas deseadas
            resultado_final = resultado_filtrado[['id','estado_id_erp','descripcion_x']].rename(columns={'descripcion_x':'descripcion'})

            resultado = resultado_final[
                ~resultado_final.apply(lambda x: 
                    ((x['descripcion'].lower() in set(df_obtener_estados_existentes['descripcion'].str.lower())) 
                    ), axis=1)
            ]

            # if resultado.empty:
            #     return {'respuesta': resultado.to_dict(orient='records'), 'estado': 0}
            
            resultado_actualizacion = resultado.to_dict(orient='records')
        else:
            resultado_actualizacion = []

        return {'respuesta':resultado_actualizacion,'estado':2} if len(resultado_actualizacion) > 0 else {'respuesta':resultado_actualizacion,'estado':0}
    
    def obtener_no_sufrieron_cambios(self):
        validacion = self.validacion_existe_informacion(self.__obtener_estados_existente)
        if validacion:
            df_estados = pd.DataFrame(self.__estados)
            df_obtener_estados_existentes = pd.DataFrame(self.__obtener_estados_existente)
            
            df_estados["estado_id_erp"] = df_estados["estado_id_erp"]
            df_obtener_estados_existentes["estado_id_erp"] = df_obtener_estados_existentes["estado_id_erp"]
            
            df_estados["descripcion"] = df_estados["descripcion"]
            df_obtener_estados_existentes["descripcion"] = df_obtener_estados_existentes["descripcion"]
            
            resultado = pd.merge(df_estados, df_obtener_estados_existentes, how='inner', on=['estado_id_erp', 'descripcion'])
            
            estados_sin_cambios = resultado.to_dict(orient='records')
        else:
            estados_sin_cambios = []

        return {'respuesta':estados_sin_cambios,'estado':3} if len(estados_sin_cambios)>0 else {'respuesta':estados_sin_cambios,'estado':0}
    
    
    def insertar_informacion(self, novedades_unidad_organizativa):
        cantidad_de_registros = len(novedades_unidad_organizativa)
        if len(novedades_unidad_organizativa) > 0:
            # session.bulk_insert_mappings(ProyectoEstado, informacion_unidad_gerencia)
            # session.commit()
            
            return  {'mensaje': f'Se han realizado {cantidad_de_registros} registros exitosos.' if cantidad_de_registros > 1 else  f'Se ha registrado un ({cantidad_de_registros}) proyecto Estado exitosamente.'}
        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        cantidad_de_actualizaciones = len(actualizacion_gerencia_unidad_organizativa)
        if len(actualizacion_gerencia_unidad_organizativa) > 0:
            # session.bulk_update_mappings(ProyectoEstado, actualizacion_gerencia_unidad_organizativa)
            # session.commit()
            
            return  {'mensaje': f'Se han realizado {cantidad_de_actualizaciones} actualizaciones exitosamente.' if cantidad_de_actualizaciones > 1 else  f'Se ha actualizado un ({cantidad_de_actualizaciones}) registro exitosamente.'}
        return "No se han actualizado datos"

    