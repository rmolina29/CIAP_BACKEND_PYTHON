import math
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from app.parametros.gerencia.model.datos_personales_model import UsuarioDatosPersonales
from app.database.db import session
from typing import List
from app.parametros.gerencia.esquema.archivo_esquema import Archivo
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
from fastapi import  UploadFile
import pandas as pd

class Gerencia:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        resultado_estructuracion = self.__proceso_de_informacion_estructuracion()
        self.__informacion_excel_duplicada = resultado_estructuracion['duplicados']
        self.__gerencia_excel = resultado_estructuracion['resultado']
        # todas las gerencias existentes en la base de datos
        self.__obtener_gerencia_existente = self.obtener()
        # gerencia que me envia el usuario a traves del excel
        self.__gerencia = self.gerencia_usuario_procesada()
        self.__validacion_contenido = len(self.__gerencia) > 0 and len(self.__obtener_gerencia_existente) > 0
    
    
    def __proceso_de_informacion_estructuracion(self):
        
        df = pd.read_excel(self.__file.file)
        # Imprimir las columnas reales del DataFrame
        df.columns = df.columns.str.strip()
        
        selected_columns = ["ERP", "Gerencia", "NIT"]

        df_excel = df[selected_columns]
        
        if df_excel.empty:
                return {'resultado': [], 'duplicados': []}

        # Cambiar los nombres de las columnas
        df_excel = df_excel.rename(
            columns={
               "ERP": "unidad_gerencia_id_erp", 
               "Gerencia": "nombre", 
                "NIT": "NIT"
            }
        )
        
        df_excel["unidad_gerencia_id_erp"] = df_excel["unidad_gerencia_id_erp"].str.lower()
        df_excel["nombre"] = df_excel["nombre"].str.lower()

        duplicados_unidad_erp = df_excel.duplicated(subset='unidad_gerencia_id_erp', keep=False)
        duplicados_nombre = df_excel.duplicated(subset='nombre', keep=False)
        # Filtrar DataFrame original
        resultado = df_excel[~(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        duplicados = df_excel[(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        
        duplicated = pd.DataFrame(duplicados)
        
        if duplicated.isnull().any(axis=1).any():
            lista_gerencias = []
        else:
            lista_gerencias = [{**item, 'NIT': int(item['NIT'])} if isinstance(item.get('NIT'), (int, float)) and not math.isnan(item.get('NIT')) else item for item in duplicados]
        
        return {'resultado':resultado,'duplicados':lista_gerencias}
        
    def validacion_informacion_gerencia_nit(self):
        try:
            if not self.__gerencia_excel:
                return {'log': [], 'gerencia_filtrada_excel': []}

            gerencia_log, gerencia_filtrada_excel = [], []
            
            for item in self.__gerencia_excel:
                if isinstance(item.get('NIT'), (int, float)):
                    gerencia_filtrada_excel.append(item)
                else:
                    gerencia_log.append(item)
            return {'log': gerencia_log, 'gerencia_filtrada_excel': gerencia_filtrada_excel}

        except Exception as e:
            raise Exception(f"Error al realizar la comparación: {str(e)}") from e

    
    
    def obtener(self):
        informacion_gerencia = session.query(ProyectoUnidadGerencia).all()
        # Convertir lista de objetos a lista de diccionarios
        gerencia_data = [gerencia.to_dict() for gerencia in informacion_gerencia]
        return gerencia_data

    def transacciones(self):
        try:
            if  self.__validacion_contenido:
                novedades_de_gerencia = self.comparacion_gerencia()
                # informacion a insertar
                lista_insert = novedades_de_gerencia.get("insercion_datos")
                gerencia_update = novedades_de_gerencia.get("actualizacion_datos")
                sin_cambios = novedades_de_gerencia.get("excepciones_campos_unicos")
                excepciones_id_usuario = novedades_de_gerencia.get("exepcion_id_usuario")

                log_transaccion_registro = self.insertar_inforacion(lista_insert)
                log_transaccion_actualizar = self.actualizar_informacion(gerencia_update)
                log_nit_invalido = self.validacion_informacion_gerencia_nit()

                log_transaccion_registro_gerencia = {
                    "log_transaccion_excel": {
                        "gerencia_registradas": log_transaccion_registro,
                        "gerencias_actualizadas": log_transaccion_actualizar,
                        "gerencias_sin_cambios": sin_cambios,
                        "gerencia_nit_no_existe": excepciones_id_usuario,
                        "gerencia_filtro_nit_invalido":log_nit_invalido['log'],
                        "gerencias_duplicadas": self.__informacion_excel_duplicada
                    }
                }

                return log_transaccion_registro_gerencia
            
            return {'Mensaje':'No hay informacion para realizar el proceso'}

        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()

    def comparacion_gerencia(self):
        try:
            (
                gerencias_nuevas,
                gerencias_actualizacion,
                no_sufieron_cambios,
                excepciones_id_usuario
            ) = self.filtrar_gerencias()

         
            resultado = {
                "insercion_datos": gerencias_nuevas,
                "actualizacion_datos": gerencias_actualizacion,
                "excepciones_campos_unicos": no_sufieron_cambios,
                "exepcion_id_usuario": excepciones_id_usuario
            }

            return resultado
        except Exception as e:
            raise (f"Error al realizar la comparación: {str(e)}") from e

    def filtrar_gerencias(self):
        excepciones_gerencia = self.obtener_no_sufrieron_cambios()
        excepciones_id_usuario = self.excepciones_id_usuario()
        gerencias_nuevas = self.filtrar_gerencias_nuevas(excepciones_gerencia)
        gerencias_actualizacion = self.obtener_gerencias_actualizacion(
            excepciones_gerencia
        )

        return (
            gerencias_nuevas,
            gerencias_actualizacion,
            excepciones_gerencia,
            excepciones_id_usuario
        )

    #  esta funcion sirve para validar lo que se envia en excel contra lo que recibe en la base de datos
    #  sacando asi los valores nuevos que no existen ninguno en la base de datos es decir se insertan
    def filtrar_gerencias_nuevas(self, excepciones_gerencia):
        try:
            # gerencias_nuevas = [
            #     item
            #     for item in self.__gerencia
            #     if (
            #         item["responsable_id"] != 0
            #         and (item["unidad_gerencia_id_erp"].lower() or item["nombre"].lower())
            #         not in {
            #             (g["unidad_gerencia_id_erp"].lower() or g["nombre"].lower())
            #             for g in self.__obtener_gerencia_existente
            #         }
            #     )
            # ]
            if self.__validacion_contenido:
                df_unidad_gerencia = pd.DataFrame(self.__gerencia)
                df_obtener_unidad_gerencia_existentes = pd.DataFrame(self.__obtener_gerencia_existente)
                
           
                
                df_unidad_gerencia['responsable_id'] = df_unidad_gerencia['responsable_id'].astype(int)
                df_unidad_gerencia['nombre'] = df_unidad_gerencia['nombre'].str.lower()
                
                df_obtener_unidad_gerencia_existentes['nombre'] = df_obtener_unidad_gerencia_existentes['nombre'].str.lower()
                df_obtener_unidad_gerencia_existentes['unidad_gerencia_id_erp'] = df_obtener_unidad_gerencia_existentes['unidad_gerencia_id_erp'].str.lower()
                
                resultado = df_unidad_gerencia[
                    (df_unidad_gerencia['responsable_id'] != 0) &
                    ~df_unidad_gerencia.apply(lambda x: ((x['unidad_gerencia_id_erp'] in set(df_obtener_unidad_gerencia_existentes['unidad_gerencia_id_erp'])) or (x['nombre'] in set(df_obtener_unidad_gerencia_existentes['nombre']))), axis=1)
                ]
                                
                nuevas_gerencias_a_registrar = resultado.to_dict(orient='records')
                
                filtro_unidad_organizativa = self.gerencias_mapeo_excepciones(
                nuevas_gerencias_a_registrar, excepciones_gerencia
                )
                
                if len(filtro_unidad_organizativa) == 0:
                    return nuevas_gerencias_a_registrar
              
            else:
                nuevas_gerencias_a_registrar = []
                
            return nuevas_gerencias_a_registrar
        
        except Exception as e:
            print(f"Se produjo un error: {str(e)}")
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e


    # Aqui se obtiene los que se pueden actualizar en la gerencia es decir los que han sufrido cambios
    def obtener_gerencias_actualizacion(self, excepciones_gerencia):
        # gerencias_actualizacion = [
        #     {
        #         "id": g["id"],
        #         "unidad_gerencia_id_erp": item["unidad_gerencia_id_erp"],
        #         "nombre": item["nombre"],
        #         "responsable_id": item["responsable_id"],
        #     }
        #     for item in self.__gerencia
        #     for g in self.__obtener_gerencia_existente
        #     if (
        #         item["responsable_id"] != 0
        #         and 
        #         item["unidad_gerencia_id_erp"].strip().lower() == g["unidad_gerencia_id_erp"].strip().lower()
        #         and 
        #         (item['nombre'].strip().lower() != g['nombre'].strip().lower() or
        #         item['responsable_id'] != g['responsable_id'])
        #     )
        # ]
        
        # print(gerencias_actualizacion)
        
        if self.__validacion_contenido:
            df_gerencia = pd.DataFrame(self.__gerencia)
            df_obtener_unidad_de_gerencia = pd.DataFrame(self.__obtener_gerencia_existente)
            
            df_gerencia['responsable_id'] = df_gerencia['responsable_id'].astype(int)
            df_gerencia['unidad_gerencia_id_erp'] = df_gerencia['unidad_gerencia_id_erp'].str.lower()
            df_obtener_unidad_de_gerencia['unidad_gerencia_id_erp'] = df_obtener_unidad_de_gerencia['unidad_gerencia_id_erp'].str.lower()
            
            resultado = pd.merge(
                df_gerencia[['unidad_gerencia_id_erp','nombre','responsable_id']],
                df_obtener_unidad_de_gerencia[['id','unidad_gerencia_id_erp','nombre','responsable_id']],
                left_on=['unidad_gerencia_id_erp'],
                right_on=['unidad_gerencia_id_erp'],
                how='inner',
            )
         
            # Seleccionar las columnas deseadas
            
            resultado_final = resultado[['id', 'unidad_gerencia_id_erp', 'nombre_x', 'responsable_id_x']].rename(columns={'nombre_x':'nombre','responsable_id_x':'responsable_id'})
            
            resultado = resultado_final[
                         ~resultado_final.apply(lambda x: 
                        ((x['nombre'] in set(df_obtener_unidad_de_gerencia['nombre'])) and
                        (x['unidad_gerencia_id_erp'] != df_obtener_unidad_de_gerencia.loc[df_obtener_unidad_de_gerencia['nombre'] == x['nombre'], 'unidad_gerencia_id_erp'].values[0])
                        ), axis=1)
                ]
            
            gerencia_actualizar = resultado.to_dict(orient='records')
        
            filtrado_actualizacion = self.gerencias_mapeo_excepciones(
                gerencia_actualizar, excepciones_gerencia
            )
            
            if len(filtrado_actualizacion) == 0:
                    return gerencia_actualizar
        else:
            filtrado_actualizacion = []
        
        return filtrado_actualizacion

    # esto son los que no han tenido nada de cambios pero lo han querido enviar a actualizar
    #  se lleva un registro de estos
    # esta validacion es para los usuarios que tienen items repetidos
    def obtener_no_sufrieron_cambios(self):
        # no_sufieron_cambios = [
        #     {
        #         "unidad_gerencia_id_erp": item["unidad_gerencia_id_erp"],
        #         "nombre": item["nombre"],
                
        #     }
        #     for item in self.__gerencia
        #     for g in self.__obtener_gerencia_existente
        #     if ((item["nombre"].upper() == g["nombre"].upper()) ) or ((item["responsable_id"] == g["responsable_id"]))
        # ]
        if self.__validacion_contenido:
            df_unidad_gerencia = pd.DataFrame(self.__gerencia)
            df_obtener_unidad_gerencia_existentes = pd.DataFrame(self.__obtener_gerencia_existente)
            
            df_unidad_gerencia['nombre'] = df_unidad_gerencia['nombre'].str.lower()
            df_obtener_unidad_gerencia_existentes['nombre'] = df_obtener_unidad_gerencia_existentes['nombre'].str.lower()
            
            no_sufren_cambios = pd.merge(
                df_unidad_gerencia[['unidad_gerencia_id_erp','nombre','responsable_id']],
                df_obtener_unidad_gerencia_existentes[['unidad_gerencia_id_erp','nombre','responsable_id']],
                on='nombre',
                how='inner'
            )

            resultado_final = no_sufren_cambios[['unidad_gerencia_id_erp_x', 'nombre', 'responsable_id_x']].rename(
                columns={'unidad_gerencia_id_erp_x': 'unidad_gerencia_id_erp', 'responsable_id_x': 'responsable_id'})
            
            gerencias_sin_cambios = resultado_final.to_dict(orient='records')
        else:
            gerencias_sin_cambios = []

        return gerencias_sin_cambios

    # esto son los que no han tenido nada de cambios pero lo han querido enviar a actualizar
    #  se lleva un registro de estos
    # esta validacion es para los usuarios que tienen items repetidos
    def excepciones_id_usuario(self):
        # id_usuario_no_existe = [
        #     {
        #         "unidad_gerencia_id_erp": item["unidad_gerencia_id_erp"],
        #         "nombre": item["nombre"],
        #     }
        #     for item in self.__gerencia
        #     if ((item["responsable_id"]) == 0)
        # ]
        
        if self.__validacion_contenido:
            df = pd.DataFrame(self.__gerencia)
            # Filtrar el DataFrame para obtener filas con valores nulos
            df_filtrado = df[(df == 0).any(axis=1)]
            # Seleccionar solo las columnas deseadas
            unidad_organizativas_columnas = ["unidad_gerencia_id_erp", "nombre"]
            df_filtrado = df_filtrado[unidad_organizativas_columnas]
            # Convertir el DataFrame filtrado a un diccionario
            id_usuario_no_existe = df_filtrado.to_dict(orient='records')
        else:
            id_usuario_no_existe = []
            
        return id_usuario_no_existe

    #  a traves de esta funcion me va a devolver la gerencia compeleta pero con el id_usuario ya que se realiza
    #  una consulta para obtener los id_usuario con la cedula, es decir con el nit del usuario
    def gerencia_usuario_procesada(self):
        try:
            gerencia_excel = self.validacion_informacion_gerencia_nit()
            resultados = []
            for gerencia in gerencia_excel['gerencia_filtrada_excel']:
                nit_value = gerencia['NIT']
                # Verificar si el valor es un número y no es NaN
                if isinstance(nit_value, (int, float)) and not math.isnan(nit_value):
                    usuario = self.query_usuario(int(nit_value))

                    resultados.append({
                        "unidad_gerencia_id_erp": gerencia["unidad_gerencia_id_erp"],
                        "nombre": gerencia["nombre"],
                        "responsable_id": usuario.to_dict().get("id_usuario") if usuario else 0,
                    })
                    
            return resultados

        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e

    def query_usuario(self, identificacion):
        return (
            session.query(UsuarioDatosPersonales)
            .filter(
                and_(
                    UsuarioDatosPersonales.identificacion == identificacion,
                    UsuarioDatosPersonales.estado == 1,
                )
            )
            .first()
        )

    def insertar_inforacion(self, novedades_de_gerencia: List):
        try:
            if len(novedades_de_gerencia) > 0:
                actualizacion_data = self.procesar_datos_minuscula(novedades_de_gerencia)
                # session.bulk_insert_mappings(ProyectoUnidadGerencia, actualizacion_data)
                return novedades_de_gerencia

            return "No se han registrado datos"
        except SQLAlchemyError as e:
              print(f"Se produjo un error de SQLAlchemy: {e}")
            #   return e
        

    def actualizar_informacion(self, actualizacion_gerencia):
        try:
            # print(f' actualizacionnn {actualizacion_gerencia is None}')
            if len(actualizacion_gerencia) > 0  :
                actualizacion_data = self.procesar_datos_minuscula(actualizacion_gerencia)
                # session.bulk_update_mappings(ProyectoUnidadGerencia, actualizacion_data)
                return actualizacion_gerencia

            return "No se han actualizado datos"
        except SQLAlchemyError as e:
              print(f"Se produjo un error de SQLAlchemy: {e}")
            #   return e

    # se realiza un mapeo para realizar el filtro de la gerencia actualizar a registrar
    # y me envia la lista de gerencias que se le va realizar la insercion o la actualizacion
    def gerencias_mapeo_excepciones(self, gerencia, excepciones):
        # filtro de excepciones atrapada de datos unicos, el cual obtiene la informacion nueva y la filtra con las exepciones que existen
        # filtro_gerencia = [
        #     item
        #     for item in gerencia
        #     if {
        #         "unidad_gerencia_id_erp": item["unidad_gerencia_id_erp"],
        #         "nombre": item["nombre"],
        #     }
        #     not in excepciones
        # ]
        df_unidad_gerencia = pd.DataFrame(gerencia)
        df_excepciones = pd.DataFrame(excepciones)
        
        df_unidad_gerencia['id'] = 0 if 'id' not in df_unidad_gerencia.columns else df_unidad_gerencia['id']
        
        columnas_necesarias = ['id', 'unidad_gerencia_id_erp', 'nombre', 'responsable_id']
        if set(columnas_necesarias).issubset(df_unidad_gerencia.columns) and set(['unidad_gerencia_id_erp', 'nombre']).issubset(df_excepciones.columns):
            resultado = pd.merge(
                df_unidad_gerencia[columnas_necesarias],
                df_excepciones[['unidad_gerencia_id_erp', 'nombre']],
                on = ['unidad_gerencia_id_erp', 'nombre'],
                how='left',
                indicator=True
            )
            # Verificar si 'unidad_gerencia_id_erp' y 'nombre' están presentes en resultado antes de continuar
            if set(['unidad_gerencia_id_erp', 'nombre']).issubset(resultado.columns):
                # Filtrar las filas donde el indicador '_merge' es 'left_only' (no está en excepciones)
                filtro_gerencia = resultado[resultado['_merge'] == 'left_only'][columnas_necesarias]
                # Convertir a lista de diccionarios
                gerencia_mapeo_resultado = filtro_gerencia.to_dict(orient='records')
            else:
                gerencia_mapeo_resultado = []
        else:
            gerencia_mapeo_resultado = []
        
        return gerencia_mapeo_resultado
    
    def procesar_datos_minuscula(self,datos):
        df = pd.DataFrame(datos)
        df[['unidad_gerencia_id_erp', 'nombre']] = df[['unidad_gerencia_id_erp', 'nombre']].apply(lambda x: x.str.lower())
        return df.to_dict(orient='records')
