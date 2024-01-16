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
        
        
        selected_data["unidad_organizativa_id_erp"] = selected_data["unidad_organizativa_id_erp"]
        selected_data["nombre"] = selected_data["nombre"]
        
        df_filtered = selected_data.dropna()

        duplicados_unidad_erp = df_filtered.duplicated(subset='unidad_organizativa_id_erp', keep=False)
        duplicados_nombre = df_filtered.duplicated(subset='nombre', keep=False)
   
        # Filtrar DataFrame original
        resultado = df_filtered[~(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        duplicados = df_filtered[(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        
        return {'resultado':resultado,'duplicados':duplicados}
    
    
    def proceso_sacar_estado(self):
            novedades_de_unidad_organizativa = self.comparacion_unidad_organizativa()
            
            lista_insert = novedades_de_unidad_organizativa.get("insercion_datos")['estado']
            unidad_organizativa_update = self.obtener_unidad_organizativa_actualizacion()['estado']
            sin_cambios = novedades_de_unidad_organizativa.get("excepciones_campos_unicos")['estado']
            excepciones_id_usuario = novedades_de_unidad_organizativa.get("exepcion_unidad_organizativa")['estado']
            gerencia_existente = novedades_de_unidad_organizativa.get("gerencias_existentes")['estado']

            # Crear un conjunto con todos los valores de estado
            estados = {lista_insert, unidad_organizativa_update, sin_cambios, excepciones_id_usuario, gerencia_existente}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados
    
    def transacciones(self):
        try:
            if self.__validacion_contenido:
                novedades_de_unidad_organizativa = self.comparacion_unidad_organizativa()
                # informacion a insertar
                
                lista_insert = novedades_de_unidad_organizativa.get("insercion_datos")['respuesta']
                
                unidad_organizativa_update = self.obtener_unidad_organizativa_actualizacion()['respuesta']
                
                sin_cambios = novedades_de_unidad_organizativa.get("excepciones_campos_unicos")['respuesta']
                
                excepciones_id_usuario = novedades_de_unidad_organizativa.get("exepcion_unidad_organizativa")['respuesta']
                
                gerencia_existente = novedades_de_unidad_organizativa.get("gerencias_existentes")['respuesta']
            

                log_transaccion_registro = self.insertar_informacion(lista_insert)
                
                log_transaccion_actualizar = self.actualizar_informacion(unidad_organizativa_update)
                
           
                
                estado_id = self.proceso_sacar_estado()
                
                log_transaccion_registro_gerencia = {
                        "log_transaccion_excel": {
                            'Respuesta':[
                                {
                                    "gerencia_registradas": log_transaccion_registro,
                                    "gerencias_actualizadas": log_transaccion_actualizar,
                                    "gerencias_sin_cambios": sin_cambios,
                                }
                            ],
                            'errores':{
                                "unidad_organizativa_id_no_existe": excepciones_id_usuario,
                                "unidad_organizativa_duplicadas": self.__informacion_excel_duplicada,
                                "gerencias_existentes":gerencia_existente
                            }
                
                        },
                        'estado':{
                            'id':estado_id
                        }
                    }

                return log_transaccion_registro_gerencia
            
            return { 'Mensaje':'No hay informacion para realizar el proceso',
                    'duplicados':self.__informacion_excel_duplicada,'estado':5}

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
        
        return {'respuesta':id_gerencias_none,'estado':4} if len(id_gerencias_none) > 0 else {'respuesta':id_gerencias_none,'estado':0}
    
    def obtener_no_sufrieron_cambios(self):
        
        if self.__validacion_contenido:
            df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
            df_obtener_unidad_organizativa_existentes = pd.DataFrame(self.__obtener_unidad_organizativa_existentes)[['unidad_organizativa_id_erp','nombre','gerencia_id']]

            df_unidad_organizativa['nombre'] = df_unidad_organizativa['nombre']
            df_obtener_unidad_organizativa_existentes['nombre'] = df_obtener_unidad_organizativa_existentes['nombre']
        
            
            no_sufren_cambios = pd.merge(
                df_unidad_organizativa,
                df_obtener_unidad_organizativa_existentes,
                how='inner',
                on = ['unidad_organizativa_id_erp','nombre','gerencia_id']
            )
            
            result = no_sufren_cambios.to_dict(orient='records')
        else:
            result = []
        
        return {'respuesta':result,'estado':3} if len(result) > 0 else {'respuesta':result,'estado':0}

    
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
                
                df_unidad_organizativa['unidad_organizativa_id_erp'] = df_unidad_organizativa['unidad_organizativa_id_erp']
                df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'] = df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp']
                
                df_unidad_organizativa['nombre'] = df_unidad_organizativa['nombre']
                df_obtener_unidad_organizativa_existentes['nombre'] = df_obtener_unidad_organizativa_existentes['nombre']
      
                
                resultado = df_unidad_organizativa[
                    (df_unidad_organizativa['gerencia_id'] != 0) &
                    ~df_unidad_organizativa.apply(lambda x: ((x['unidad_organizativa_id_erp'].lower() in set(df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'].str.lower())) 
                                                             or 
                                                             (x['nombre'].lower() in set(df_obtener_unidad_organizativa_existentes['nombre'].str.lower()))
                                                             ), axis=1)
                                                            ]
                
                
                direccion_nuevas = resultado[['unidad_organizativa_id_erp', 'nombre','gerencia_id']].to_dict(orient='records')
            
        
                # filtro de excepciones atrapada de datos unicos, el cual obtiene la informacion nueva y la filtra con las exepciones que existen
                filtro_unidad_organizativa = self.direccion_mapeo_excepciones(
                direccion_nuevas, excepciones_unidad_organizativa
                )
                
                if len(filtro_unidad_organizativa) == 0:
                    return {'respuesta':direccion_nuevas,'estado':1} if len(direccion_nuevas) > 0 else {'respuesta':direccion_nuevas,'estado':0}
                
                df_registro = pd.DataFrame(filtro_unidad_organizativa)
                registros = df_registro.drop(columns=['id']) 
                registros_unidad_organizativa = registros.to_dict(orient='records')
                
            else:
                registros_unidad_organizativa = []
            
            return {'respuesta':registros_unidad_organizativa,'estado':1} if len(registros_unidad_organizativa)>0 else {'respuesta':registros_unidad_organizativa,'estado':0}

        except Exception as e:
            print(f"Se produjo un error: {str(e)}")
        
    def obtener_unidad_organizativa_actualizacion(self):

        if self.__validacion_contenido:
            df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
            df_obtener_unidad_organizativa_existentes = pd.DataFrame(self.__obtener_unidad_organizativa_existentes)

            df_unidad_organizativa['gerencia_id'] = df_unidad_organizativa['gerencia_id'].astype(int)
            
            df_unidad_organizativa['nombre'] = df_unidad_organizativa['nombre']
            df_obtener_unidad_organizativa_existentes['nombre'] = df_obtener_unidad_organizativa_existentes['nombre']
            
            df_unidad_organizativa['unidad_organizativa_id_erp'] = df_unidad_organizativa['unidad_organizativa_id_erp']
            df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'] = df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp']
            
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
                (x['nombre'].lower()  in set(df_obtener_unidad_organizativa_existentes['nombre'].str.lower())) and
                (x['unidad_organizativa_id_erp'] != df_obtener_unidad_organizativa_existentes.loc[df_obtener_unidad_organizativa_existentes['nombre'].str.lower() == x['nombre'].lower(), 'unidad_organizativa_id_erp'].values[0])
            ), axis=1)
        ]
                    
            if direccion_actualizar.empty:
                return {'respuesta':[],'estado':0}
            
            direccion_culmnas = direccion_actualizar[['id','nombre','gerencia_id']]
            filtrado_actualizacion = direccion_culmnas.to_dict(orient='records')
            filtro_direccion = self.obtener_no_sufrieron_cambios()['respuesta']
            
            if len(filtro_direccion) != 0:
                df_unidad_organizativa = pd.DataFrame(filtro_direccion)
                columnas_filtrar = ['nombre', 'gerencia_id']
                df_filtrado = direccion_actualizar[~direccion_actualizar[columnas_filtrar].isin(df_unidad_organizativa[columnas_filtrar].to_dict('list')).all(axis=1)]
                filtrado_actualizacion = df_filtrado.to_dict(orient='records')
                return {'respuesta':filtrado_actualizacion,'estado':2} if len(filtrado_actualizacion) > 0 else {'respuesta':filtrado_actualizacion,'estado':0}
            
        return {'respuesta':filtrado_actualizacion,'estado':2} if len(filtrado_actualizacion) > 0 else {'respuesta':filtrado_actualizacion,'estado':0}

    
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
        try:
            if len(novedades_unidad_organizativa) > 0:
                session.bulk_insert_mappings(ProyectoUnidadOrganizativa, novedades_unidad_organizativa)
                return novedades_unidad_organizativa

            return "No se han registrado datos"
        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        try:
        
            if len(actualizacion_gerencia_unidad_organizativa) > 0:
                session.bulk_update_mappings(ProyectoUnidadOrganizativa, actualizacion_gerencia_unidad_organizativa)
                return actualizacion_gerencia_unidad_organizativa

            return "No se han actualizado datos"
        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()
    
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
        
        
    
    def gerencias_existentes(self):
        
        validacion = self.__validacion_contenido
        
        if validacion:
            df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
            df_obtener_unidad_organizativa_existentes = pd.DataFrame(self.__obtener_unidad_organizativa_existentes)
            
            df_unidad_organizativa['nombre'] = df_unidad_organizativa['nombre']
            df_obtener_unidad_organizativa_existentes['nombre'] = df_obtener_unidad_organizativa_existentes['nombre']
            
            df_unidad_organizativa['unidad_organizativa_id_erp'] = df_unidad_organizativa['unidad_organizativa_id_erp']
            df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'] = df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp']
            

            
            direcciones_existentes = df_unidad_organizativa[
                    df_unidad_organizativa.apply(lambda x: 
                        ((x['nombre'].lower() in set(df_obtener_unidad_organizativa_existentes['nombre'].str.lower())) and
                        (x['unidad_organizativa_id_erp'].lower()  != df_obtener_unidad_organizativa_existentes.loc[df_obtener_unidad_organizativa_existentes['nombre'].str.lower()  == x['nombre'].lower(), 'unidad_organizativa_id_erp'].str.lower().values[0])
                        ), axis=1)
                    |
                    df_unidad_organizativa['unidad_organizativa_id_erp'].str.lower().isin(df_obtener_unidad_organizativa_existentes['unidad_organizativa_id_erp'].str.lower().values)
                ]
            
            actualizar_ = self.obtener_unidad_organizativa_actualizacion()['respuesta']
            
            if len(actualizar_) > 0:
                df_cliente = pd.DataFrame(actualizar_)
                df_filtrado = direcciones_existentes[~direcciones_existentes[['nombre','gerencia_id']].isin(df_cliente[['nombre','gerencia_id']].to_dict('list')).all(axis=1)]
                filtro_direccion =  df_filtrado.to_dict(orient='records')
                return {'respuesta':filtro_direccion,'estado':3} if len(filtro_direccion) > 0 else {'respuesta':filtro_direccion,'estado':0}
            
            gerencia_filtro = self.obtener_no_sufrieron_cambios()['respuesta']
            obtener_excepcion = direcciones_existentes.to_dict(orient='records')
 
            
            if len(gerencia_filtro) != 0:
                df_cliente = pd.DataFrame(gerencia_filtro)
                df_filtrado = direcciones_existentes[~direcciones_existentes[['unidad_organizativa_id_erp','nombre','gerencia_id']].isin(df_cliente[['unidad_organizativa_id_erp','nombre','gerencia_id']].to_dict('list')).all(axis=1)]
                filtro_direccion =  df_filtrado.to_dict(orient='records')
                return {'respuesta':filtro_direccion,'estado':3} if len(filtro_direccion) > 0 else {'respuesta':filtro_direccion,'estado':0}
            obtener_excepcion = direcciones_existentes.to_dict(orient='records')
        else:
            obtener_excepcion = []
        return {'respuesta':obtener_excepcion,'estado':4} if len(obtener_excepcion) > 0 else {'respuesta':obtener_excepcion,'estado':0}