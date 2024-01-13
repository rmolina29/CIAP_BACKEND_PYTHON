from typing import List
from app.parametros.direccion.esquema.proyecto_unidad_organizativa import Archivo
from app.parametros.direccion.model.proyecto_unidad_organizativa import ProyectoUnidadOrganizativa
from app.database.db import session
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from fastapi import  UploadFile

class Direccion:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        resultado_estructuracion = self.__proceso_de_informacion_estructuracion()
        self.__informacion_excel_duplicada = resultado_estructuracion['duplicados']
        self.__direcion_excel = resultado_estructuracion['resultado']
        self.__obtener_unidad_organizativa_existentes = self.obtener_direccion()
        self.__unidad_organizativa = self.proceso_informacion_con_id_gerencia()
        self.__validacion_contenido = len(self.__unidad_organizativa) > 0 and len(self.__obtener_unidad_organizativa_existentes) > 0
        
    
    def __proceso_de_informacion_estructuracion(self):
        df = pd.read_excel(self.__file.file)
        # Imprimir las columnas reales del DataFrame
        df.columns = df.columns.str.strip()
        selected_columns = ["ID Dirección (ERP)", "Dirección", "ID Gerencia (ERP)"]

        selected_data = df[selected_columns]
        # Cambiar los nombres de las columnas
        selected_data = selected_data.rename(
            columns={
                "ID Dirección (ERP)": "unidad_organizativa_id_erp",
                "Dirección": "nombre",
                "ID Gerencia (ERP)": "unidad_gerencia_id_erp",
            }
        )
        
        selected_data["unidad_organizativa_id_erp"] = selected_data["unidad_organizativa_id_erp"].str.lower()
        selected_data["nombre"] = selected_data["nombre"].str.lower()
        
        # df_filtered = selected_data.dropna()

        duplicados_unidad_erp = selected_data.duplicated(subset='unidad_organizativa_id_erp', keep=False)
        duplicados_nombre = selected_data.duplicated(subset='nombre', keep=False)
   
        # Filtrar DataFrame original
        resultado = selected_data[~(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        duplicados = selected_data[(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        
        return {'resultado':resultado,'duplicados':duplicados}
    
    def transacciones(self):
        try:
            novedades_de_unidad_organizativa = self.comparacion_unidad_organizativa()
            # informacion a insertar
            lista_insert = novedades_de_unidad_organizativa.get("insercion_datos")
            unidad_organizativa_update = self.obtener_unidad_organizativa_actualizacion()
            sin_cambios = novedades_de_unidad_organizativa.get("excepciones_campos_unicos")
            excepciones_id_usuario = novedades_de_unidad_organizativa.get("exepcion_unidad_organizativa")
            gerencia_existente = novedades_de_unidad_organizativa.get("gerencias_existentes")

            log_transaccion_registro = self.insertar_informacion(lista_insert)
            log_transaccion_actualizar = self.actualizar_informacion(unidad_organizativa_update)
            
            log_transaccion_registro_unidad_organizativa = {
                "log_transaccion_excel": {
                    "unidad_organizativa_registradas": log_transaccion_registro,
                    "unidad_organizativas_actualizadas": log_transaccion_actualizar,
                    "unidad_organizativas_sin_cambios": sin_cambios,
                    "unidad_organizativa_id_no_existe": excepciones_id_usuario,
                    "unidad_organizativa_duplicadas": self.__informacion_excel_duplicada,
                    "gerencias_existentes":gerencia_existente
                }
            }

            return log_transaccion_registro_unidad_organizativa

        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()
      
    
    def obtener_direccion(self):
            informacion_direccion = session.query(ProyectoUnidadOrganizativa).all()
            gerencia_data = [gerencia.to_dict() for gerencia in informacion_direccion]
            return gerencia_data
        
        
    def comparacion_unidad_organizativa(self):
        try:
            (
                excepciones_unidad_organizativa,
                unidad_organizativa_id_erp,
                registro_unidad_organizativa,
                excepciones_datos_unicos
            ) = self.filtrar_unidad_organizativa()

            resultado = {
                "insercion_datos": registro_unidad_organizativa,
                "excepciones_campos_unicos": excepciones_unidad_organizativa,
                "exepcion_unidad_organizativa": unidad_organizativa_id_erp,
                "gerencias_existentes":excepciones_datos_unicos
            }

            return resultado
        except Exception as e:
            raise (f"Error al realizar la comparación: {str(e)}")
      
    def filtrar_unidad_organizativa(self):
        excepciones_unidad_organizativa = self.obtener_no_sufrieron_cambios()
        unidad_organizativa_id_erp = self.unidad_organizativa_id_erp()
        registro_unidad_organizativa = self.filtrar_unidad_organizativas_nuevas(excepciones_unidad_organizativa)
        excepciones_datos_unicos = self.gerencias_existentes()

        return (
            excepciones_unidad_organizativa,
            unidad_organizativa_id_erp,
            registro_unidad_organizativa,
            excepciones_datos_unicos
        )
    
    def unidad_organizativa_id_erp(self):
        if self.__validacion_contenido:
            df = pd.DataFrame(self.__unidad_organizativa)
            # Filtrar el DataFrame para obtener filas con valores nulos
            df_filtrado = df[(df == 0).any(axis=1)]
            # Seleccionar solo las columnas deseadas
            unidad_organizativas_columnas = ["unidad_organizativa_id_erp", "nombre"]
            df_filtrado = df_filtrado[unidad_organizativas_columnas]
            # Convertir el DataFrame filtrado a un diccionario
            id_gerencias_none = df_filtrado.to_dict(orient='records')
        else:
            id_gerencias_none =[] 
        
        return id_gerencias_none
    
    def obtener_no_sufrieron_cambios(self):
        
        if self.__validacion_contenido:
            df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
            df_obtener_unidad_organizativa_existentes = pd.DataFrame(self.__obtener_unidad_organizativa_existentes)[['unidad_organizativa_id_erp','nombre','gerencia_id']]

            df_unidad_organizativa['nombre'] = df_unidad_organizativa['nombre'].str.lower()
            df_obtener_unidad_organizativa_existentes['nombre'] = df_obtener_unidad_organizativa_existentes['nombre'].str.lower()
        
            
            no_sufren_cambios = pd.merge(
                df_unidad_organizativa,
                df_obtener_unidad_organizativa_existentes,
                how='inner',
                on = ['unidad_organizativa_id_erp','nombre','gerencia_id']
            )
            
            result = no_sufren_cambios.to_dict(orient='records')
        else:
            result = []
        
        return result
    
    def obtener_gerencia(self,unidad_gerencia_id_erp):
        return (
            session.query(ProyectoUnidadGerencia)
            .filter(
                and_(
                    ProyectoUnidadGerencia.unidad_gerencia_id_erp == unidad_gerencia_id_erp,
                    ProyectoUnidadGerencia.estado == 1,
                )
            )
            .first()
        )
        
    def filtrar_unidad_organizativas_nuevas(self, excepciones_unidad_organizativa):
        try:
            
            if self.__validacion_contenido:
            
                df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
                df_obtener_unidad_organizativa_existentes = pd.DataFrame(self.__obtener_unidad_organizativa_existentes)

                df_unidad_organizativa['gerencia_id'] = df_unidad_organizativa['gerencia_id'].astype(int)
                
                df_unidad_organizativa['unidad_organizativa_id_erp'] = df_unidad_organizativa['unidad_organizativa_id_erp'].str.lower()
                df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'] = df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'].str.lower()
                
                df_unidad_organizativa['nombre'] = df_unidad_organizativa['nombre'].str.lower()
                df_obtener_unidad_organizativa_existentes['nombre'] = df_obtener_unidad_organizativa_existentes['nombre'].str.lower()
      
                
                resultado = df_unidad_organizativa[
                    (df_unidad_organizativa['gerencia_id'] != 0) &
                    ~df_unidad_organizativa.apply(lambda x: ((x['unidad_organizativa_id_erp'] in set(df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'])) 
                                                             or 
                                                             (x['nombre'] in set(df_obtener_unidad_organizativa_existentes['nombre']))
                                                             ), axis=1)
                                                            ]
                
                
                direccion_nuevas = resultado[['unidad_organizativa_id_erp', 'nombre','gerencia_id']].to_dict(orient='records')
            
        
                # filtro de excepciones atrapada de datos unicos, el cual obtiene la informacion nueva y la filtra con las exepciones que existen
                filtro_unidad_organizativa = self.direccion_mapeo_excepciones(
                direccion_nuevas, excepciones_unidad_organizativa
                )
                
                if len(filtro_unidad_organizativa) == 0:
                    return direccion_nuevas
                
                df_registro = pd.DataFrame(filtro_unidad_organizativa)
                registros = df_registro.drop(columns=['id']) 
                registros_unidad_organizativa = registros.to_dict(orient='records')
                
            else:
                registros_unidad_organizativa = []
            
            return registros_unidad_organizativa
        except Exception as e:
            print(f"Se produjo un error: {str(e)}")
        
    def obtener_unidad_organizativa_actualizacion(self):

        if self.__validacion_contenido:
            df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
            df_obtener_unidad_organizativa_existentes = pd.DataFrame(self.__obtener_unidad_organizativa_existentes)

            df_unidad_organizativa['gerencia_id'] = df_unidad_organizativa['gerencia_id'].astype(int)
            
            df_unidad_organizativa['nombre'] = df_unidad_organizativa['nombre'].str.lower()
            df_obtener_unidad_organizativa_existentes['nombre'] = df_obtener_unidad_organizativa_existentes['nombre'].str.lower()
            
            df_unidad_organizativa['unidad_organizativa_id_erp'] = df_unidad_organizativa['unidad_organizativa_id_erp'].str.lower()
            df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'] = df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'].str.lower()
            
            resultado = pd.merge(
                df_unidad_organizativa[['unidad_organizativa_id_erp','nombre','gerencia_id']],
                df_obtener_unidad_organizativa_existentes[['id','unidad_organizativa_id_erp','nombre','gerencia_id']],
                left_on=['unidad_organizativa_id_erp'],
                right_on=['unidad_organizativa_id_erp'],
                how='inner'
            )

            resultado_final = resultado[['id', 'unidad_organizativa_id_erp', 'nombre_x', 'gerencia_id_x']].rename(columns={'nombre_x':'nombre','gerencia_id_x':'gerencia_id'})
            
            columnas_filtrar = ['unidad_organizativa_id_erp', 'nombre', 'gerencia_id']

            direccion_actualizar = resultado_final[
                          ~resultado_final.apply(lambda x: (
                            (x['nombre'] in set(df_obtener_unidad_organizativa_existentes['nombre'])) and
                            (x['unidad_organizativa_id_erp'] != df_obtener_unidad_organizativa_existentes.loc[df_obtener_unidad_organizativa_existentes['nombre'] == x['nombre'], 'unidad_organizativa_id_erp'].values[0]) 
                            or
                            # filtro para la misma unidad de gerencia pero con diferente id de responsable
                            (x['gerencia_id'] in set(df_obtener_unidad_organizativa_existentes['gerencia_id'])) and
                            (x['unidad_organizativa_id_erp'] == df_obtener_unidad_organizativa_existentes.loc[df_obtener_unidad_organizativa_existentes['gerencia_id'] != x['gerencia_id'], 'unidad_organizativa_id_erp'].values[0])
                        ), axis=1)
                    ]
            
            filtrado_actualizacion = direccion_actualizar.to_dict(orient='records')
            
            filtro_direccion = self.obtener_no_sufrieron_cambios()
            
            if len(filtro_direccion) != 0:
                df_unidad_organizativa = pd.DataFrame(filtro_direccion)
                columnas_filtrar = ['unidad_organizativa_id_erp', 'nombre', 'gerencia_id']
                df_filtrado = direccion_actualizar[~direccion_actualizar[columnas_filtrar].isin(df_unidad_organizativa[columnas_filtrar].to_dict('list')).all(axis=1)]
                return df_filtrado.to_dict(orient='records')
        
        
        return filtrado_actualizacion
    
    def direccion_mapeo_excepciones(self, direccion, excepciones):
        
        df_unidad_organizativa = pd.DataFrame(direccion)
        df_excepciones = pd.DataFrame(excepciones)
        df_unidad_organizativa['id'] = 0 if 'id' not in df_unidad_organizativa.columns else df_unidad_organizativa['id']
        
        columnas_necesarias = ['id', 'unidad_organizativa_id_erp', 'nombre', 'gerencia_id']
        if set(columnas_necesarias).issubset(df_unidad_organizativa.columns) and set(['unidad_gerencia_id_erp', 'nombre']).issubset(df_excepciones.columns):
            resultado = pd.merge(
                    df_unidad_organizativa[['id','unidad_organizativa_id_erp', 'nombre','gerencia_id']],
                    df_excepciones[['unidad_organizativa_id_erp', 'nombre']],
                    on=['unidad_organizativa_id_erp', 'nombre'],
                    how='left',
                    indicator=True
                )
            # Verificar si 'unidad_gerencia_id_erp' y 'nombre' están presentes en resultado antes de continuar
            if set(['unidad_organizativa_id_erp', 'nombre']).issubset(resultado.columns):
                # Filtrar las filas donde el indicador '_merge' es 'left_only' (no está en excepciones)
                filtro_direccion = resultado[resultado['_merge'] == 'left_only'][columnas_necesarias]
                # Convertir a lista de diccionarios
                direccion = filtro_direccion.to_dict(orient='records')
            else:
                direccion = []
        else:
            direccion = []
        
        return direccion
    
    def insertar_informacion(self, novedades_unidad_organizativa: List):
        if len(novedades_unidad_organizativa) > 0:
            # informacion_unidad_gerencia = self.procesar_datos_minuscula(novedades_unidad_organizativa)
            # session.bulk_insert_mappings(ProyectoUnidadOrganizativa, informacion_unidad_gerencia)
            return novedades_unidad_organizativa

        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        if len(actualizacion_gerencia_unidad_organizativa) > 0:
            # informacion_unidad_gerencia = self.procesar_datos_minuscula(actualizacion_gerencia_unidad_organizativa)
            # session.bulk_update_mappings(ProyectoUnidadOrganizativa, informacion_unidad_gerencia)
            return actualizacion_gerencia_unidad_organizativa

        return "No se han actualizado datos"
    
    def proceso_informacion_con_id_gerencia(self):
        try:
            resultados = []
            for unidad_organizativa in self.__direcion_excel:
                gerencia = self.obtener_gerencia(unidad_organizativa["unidad_gerencia_id_erp"])
                if gerencia:
                    resultados.append(
                        {
                            "unidad_organizativa_id_erp": unidad_organizativa[
                                "unidad_organizativa_id_erp"
                            ],
                            "nombre": unidad_organizativa["nombre"],
                            "gerencia_id": gerencia.to_dict().get("id"),
                        }
                    )
                else:
                    resultados.append(
                        {
                            "unidad_organizativa_id_erp": unidad_organizativa[
                                "unidad_organizativa_id_erp"
                            ],
                            "nombre": unidad_organizativa["nombre"],
                            "gerencia_id": 0,
                        }
                    )
            return resultados
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e
        
        
    # def procesar_datos_minuscula(self,datos):
    #     df = pd.DataFrame(datos)
    #     df[['unidad_organizativa_id_erp', 'nombre']] = df[['unidad_organizativa_id_erp', 'nombre']].apply(lambda x: x.str.lower())
    #     return df.to_dict(orient='records')
    
    
    def gerencias_existentes(self):
        
        validacion = self.__validacion_contenido
        
        if validacion:
            df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
            df_obtener_unidad_organizativa_existentes = pd.DataFrame(self.__obtener_unidad_organizativa_existentes)
            
            df_unidad_organizativa['nombre'] = df_unidad_organizativa['nombre'].str.lower()
            df_obtener_unidad_organizativa_existentes['nombre'] = df_obtener_unidad_organizativa_existentes['nombre'].str.lower()
            
            df_unidad_organizativa['unidad_organizativa_id_erp'] = df_unidad_organizativa['unidad_organizativa_id_erp'].str.lower()
            df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'] = df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'].str.lower()
            

            
            direcciones_existentes = df_unidad_organizativa[
                    df_unidad_organizativa.apply(lambda x: 
                        ((x['nombre'] in set(df_obtener_unidad_organizativa_existentes['nombre'])) and
                        (x['unidad_organizativa_id_erp'] != df_obtener_unidad_organizativa_existentes.loc[df_obtener_unidad_organizativa_existentes['nombre'] == x['nombre'], 'unidad_organizativa_id_erp'].values[0])
                        ), axis=1)
                ]
            
            obtener_excepcion = direcciones_existentes.to_dict(orient='records')
        else:
            obtener_excepcion = []
            
        return obtener_excepcion