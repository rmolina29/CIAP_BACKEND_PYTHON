
from fastapi import  UploadFile
import pandas as pd
from app.database.db import session
from app.parametros.estado.model.estado_model import ProyectoEstado


class Estado:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        # data para la comparacion de informacion sacandola de la base de datos
        self.__obtener_estados_existente = self.obtener()
        estados_usuario_excel = self.__proceso_de_informacion_estructuracion()
        self.__estados = estados_usuario_excel['resultado']
        self.__estados_duplicados = estados_usuario_excel['duplicados']
    
    def transacciones(self):
        
        actualizacion_estados = self.filtro_estado_actualizar()
        registro_estados = self.estados_nuevos()
        
        log_transaccion_registro_gerencia = {
                "log_transaccion_excel": {
                   'sin_cambios':self.obtener_no_sufrieron_cambios(),
                #    'data_bd':self.__obtener_estados_existente,
                #    'usuario_data':self.__estados,
                   'registrar':self.insertar_informacion(registro_estados),
                   'actualizacion':self.actualizar_informacion(actualizacion_estados),
                   'duplicados':self.__estados_duplicados,
                }
        }
        
        return log_transaccion_registro_gerencia
    
    def __proceso_de_informacion_estructuracion(self):
        df = pd.read_excel(self.__file.file)
        df.columns = df.columns.str.strip()
        # Imprimir las columnas reales del DataFrame
        selected_columns = ["ID Estado (ERP)", "Estado"]

        selected_data = df[selected_columns]
        
          # Cambiar los nombres de las columnas
        selected_data = selected_data.rename(
            columns={
               "ID Estado (ERP)": "estado_id_erp", 
                "Estado": "descripcion" 
            }
        )
        
        selected_data["estado_id_erp"] = selected_data["estado_id_erp"]
        selected_data["descripcion"] = selected_data["descripcion"]
        
        
        estado_duplicado = selected_data.duplicated(subset='estado_id_erp', keep=False)
        duplicado_descripcion = selected_data.duplicated(subset='descripcion', keep=False)
        # Filtrar DataFrame original
        resultado = selected_data[~(estado_duplicado | duplicado_descripcion)].to_dict(orient='records')
        duplicados = selected_data[(estado_duplicado | duplicado_descripcion)].to_dict(orient='records')
        
        return {'resultado':resultado,'duplicados':duplicados}
    
    def obtener(self):
        estados = session.query(ProyectoEstado).all()
        # Convertir lista de objetos a lista de diccionarios
        estado = [estado.to_dict() for estado in estados ]
        return estado
    
    
    def validacion_existe_informacion(self)->bool:
        return len(self.__estados) > 0 and len(self.__obtener_estados_existente)>0
    
    
    
    def estados_nuevos(self):
        
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_estado = pd.DataFrame(self.__estados)
            df_obtener_estado_existentes = pd.DataFrame(self.__obtener_estados_existente)
            
            df_estado["estado_id_erp"] = df_estado["estado_id_erp"]
            df_estado["descripcion"] = df_estado["descripcion"]

            df_obtener_estado_existentes["estado_id_erp"] = df_obtener_estado_existentes["estado_id_erp"]
            df_obtener_estado_existentes["descripcion"] = df_obtener_estado_existentes["descripcion"]
                
            resultado = df_estado[
                    ~df_estado.apply(lambda x: 
                        ((x['estado_id_erp'] in set(df_obtener_estado_existentes['estado_id_erp'])) 
                         or 
                         (x['descripcion'] in set(df_obtener_estado_existentes['descripcion']))
                         ), axis=1)
                ]
                                
            # Resultado final
            estado_registro = resultado.to_dict(orient='records')
            
        else:
            estado_registro = []
            
        return estado_registro
    
    
    def filtro_estado_actualizar(self):
        
        validacion = self.validacion_existe_informacion()
        
        if validacion:
            df_estado = pd.DataFrame(self.__estados)
            df_obtener_estados_existentes = pd.DataFrame(self.__obtener_estados_existente)
            
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
            resultado_final = resultado_filtrado[['id', 'estado_id_erp', 'descripcion_x']].rename(columns={'descripcion_x':'descripcion'})
            
            resultado = resultado_final[
                        ~resultado_final.apply(lambda x: 
                            ((x['descripcion'] in set(df_obtener_estados_existentes['descripcion'])) 
                            ), axis=1)
                    ]
            
            resultado_actualizacion = resultado.to_dict(orient='records')
      
        else:
            resultado_actualizacion = []
            
        return resultado_actualizacion
    
    def obtener_no_sufrieron_cambios(self):
        validacion = self.validacion_existe_informacion()
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

        return estados_sin_cambios
    
    
    def insertar_informacion(self, novedades_unidad_organizativa):
        if len(novedades_unidad_organizativa) > 0:
            # session.bulk_insert_mappings(ProyectoEstado, informacion_unidad_gerencia)
            # session.flush()
            # nuevos = session.new
            
            # print("Entidades nuevas:", nuevos)
            
            return novedades_unidad_organizativa
        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        if len(actualizacion_gerencia_unidad_organizativa) > 0:
            # informacion_unidad_gerencia = self.procesar_datos_minuscula(actualizacion_gerencia_unidad_organizativa)
            # session.bulk_update_mappings(ProyectoEstado, informacion_unidad_gerencia)
            # session.flush()
            # modificaciones = session.dirty
            # print("Entidades modificadas:", modificaciones)
            
            return actualizacion_gerencia_unidad_organizativa
        return "No se han actualizado datos"
    
    # def procesar_datos_minuscula(self,datos):
    #     df = pd.DataFrame(datos)
    #     df[['estado_id_erp', 'descripcion']] = df[['estado_id_erp', 'descripcion']].apply(lambda x: x.str.lower())
    #     return df.to_dict(orient='records')
    