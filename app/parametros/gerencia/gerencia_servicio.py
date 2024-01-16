import math
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from app.parametros.gerencia.model.datos_personales_model import UsuarioDatosPersonales
from app.database.db import session
from typing import List
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
        
        df_excel["unidad_gerencia_id_erp"] = df_excel["unidad_gerencia_id_erp"]
        df_excel["nombre"] = df_excel["nombre"]
        
        df_filtered = df_excel.dropna()


        duplicados_unidad_erp = df_filtered.duplicated(subset='unidad_gerencia_id_erp', keep=False)
        duplicados_nombre = df_filtered.duplicated(subset='nombre', keep=False)
        # Filtrar DataFrame original
        resultado = df_filtered[~(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        duplicados = df_filtered[(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        
        duplicated = pd.DataFrame(duplicados)
        
        if duplicated.isnull().any(axis=1).any():
            lista_gerencias = []
        else:
            lista_gerencias = [{**item, 'NIT': int(item['NIT'])} if isinstance(item.get('NIT'), (int, float)) and not math.isnan(item.get('NIT')) else item for item in duplicados]
        
        return {'resultado':resultado,'duplicados':lista_gerencias}
        
    def validacion_informacion_gerencia_nit(self):
        try:
            if not self.__gerencia_excel:
                return {'log': [], 'gerencia_filtrada_excel': [],'estado':0}

            gerencia_log, gerencia_filtrada_excel = [], []
            
            for item in self.__gerencia_excel:
                if isinstance(item.get('NIT'), (int, float)):
                    gerencia_filtrada_excel.append(item)
                else:
                    gerencia_log.append(item)
            return {'log': gerencia_log, 'gerencia_filtrada_excel': gerencia_filtrada_excel,'estado':3}

        except Exception as e:
            raise Exception(f"Error al realizar la comparación: {str(e)}") from e

    
    
    def obtener(self):
        informacion_gerencia = session.query(ProyectoUnidadGerencia).all()
        # Convertir lista de objetos a lista de diccionarios
        gerencia_data = [gerencia.to_dict() for gerencia in informacion_gerencia]
        return gerencia_data

        
    def proceso_sacar_estado(self):
            novedades_de_gerencia = self.comparacion_gerencia()
            
            lista_insert = novedades_de_gerencia.get("insercion_datos")['estado']
            gerencia_update = novedades_de_gerencia.get("actualizacion_datos")['estado']
            sin_cambios = novedades_de_gerencia.get("excepciones_campos_unicos")['estado']
            excepciones_id_usuario = novedades_de_gerencia.get("exepcion_id_usuario")['estado']
            excepciones_gerencia = novedades_de_gerencia.get("excepcion_gerencia_existentes")['estado']
            log_nit_invalido = self.validacion_informacion_gerencia_nit()['estado']

            # Crear un conjunto con todos los valores de estado
            estados = {lista_insert, gerencia_update, sin_cambios, excepciones_id_usuario, excepciones_gerencia, log_nit_invalido}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados
    
    def transacciones(self):
        try:
            if  self.__validacion_contenido:
                novedades_de_gerencia = self.comparacion_gerencia()
                # informacion a insertar
                lista_insert = novedades_de_gerencia.get("insercion_datos")['respuesta']
                
                gerencia_update = novedades_de_gerencia.get("actualizacion_datos")['respuesta']
                
                sin_cambios = novedades_de_gerencia.get("excepciones_campos_unicos")['respuesta']
                
                excepciones_id_usuario = novedades_de_gerencia.get("exepcion_id_usuario")['respuesta']
                
                excepciones_gerencia = novedades_de_gerencia.get("excepcion_gerencia_existentes")['respuesta']

                log_transaccion_registro = self.insertar_inforacion(lista_insert)
                log_transaccion_actualizar = self.actualizar_informacion(gerencia_update)
                
                log_nit_invalido = self.validacion_informacion_gerencia_nit()
                
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
                        "gerencia_nit_no_existe": excepciones_id_usuario,
                        "gerencia_filtro_nit_invalido":log_nit_invalido['log'],
                        "gerencias_existentes":excepciones_gerencia,
                        "gerencias_duplicadas": self.__informacion_excel_duplicada
                        }
            
                    },
                    'estado':{
                        'id':estado_id
                    }
                }

                return log_transaccion_registro_gerencia
            
            return { 'Mensaje':'No hay informacion para realizar el proceso',
                    'duplicados':self.__informacion_excel_duplicada,'estado':3}

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
                excepciones_id_usuario,
                excepciones_existente_gerencia
            ) = self.filtrar_gerencias()

         
            resultado = {
                "insercion_datos": gerencias_nuevas,
                "actualizacion_datos": gerencias_actualizacion,
                "excepciones_campos_unicos": no_sufieron_cambios,
                "exepcion_id_usuario": excepciones_id_usuario,
                "excepcion_gerencia_existentes":excepciones_existente_gerencia
            }

            return resultado
        except Exception as e:
            raise (f"Error al realizar la comparación: {str(e)}") from e

    def filtrar_gerencias(self):
        excepciones_gerencia = self.obtener_no_sufrieron_cambios()
        excepciones_id_usuario = self.excepciones_id_usuario()
        gerencias_nuevas = self.filtrar_gerencias_nuevas(excepciones_gerencia)
        gerencias_actualizacion = self.obtener_gerencias_actualizacion()
        excepciones_existente_gerencia = self.obtener_excepciones_datos_unicos()

        return (
            gerencias_nuevas,
            gerencias_actualizacion,
            excepciones_gerencia,
            excepciones_id_usuario,
            excepciones_existente_gerencia
        )

    #  esta funcion sirve para validar lo que se envia en excel contra lo que recibe en la base de datos
    #  sacando asi los valores nuevos que no existen ninguno en la base de datos es decir se insertan
    def filtrar_gerencias_nuevas(self, excepciones_gerencia):
        try:
            if self.__validacion_contenido:
                df_unidad_gerencia = pd.DataFrame(self.__gerencia)
                df_obtener_unidad_gerencia_existentes = pd.DataFrame(self.__obtener_gerencia_existente)
                
                df_unidad_gerencia['responsable_id'] = df_unidad_gerencia['responsable_id'].astype(int)
                df_unidad_gerencia['nombre'] = df_unidad_gerencia['nombre']
                
                df_obtener_unidad_gerencia_existentes['nombre'] = df_obtener_unidad_gerencia_existentes['nombre']
                df_obtener_unidad_gerencia_existentes['unidad_gerencia_id_erp'] = df_obtener_unidad_gerencia_existentes['unidad_gerencia_id_erp']
                
                resultado = df_unidad_gerencia[
                    (df_unidad_gerencia['responsable_id'] != 0) &
                    ~df_unidad_gerencia.apply(lambda x: ((x['unidad_gerencia_id_erp'].lower() in set(df_obtener_unidad_gerencia_existentes['unidad_gerencia_id_erp'].str.lower())) 
                                                         or 
                                                         (x['nombre'].lower() in set(df_obtener_unidad_gerencia_existentes['nombre'].str.lower()))), axis=1)
                ]
                
                nuevas_gerencias_a_registrar = resultado.to_dict(orient='records')
                
                filtro_unidad_organizativa = self.gerencias_mapeo_excepciones(
                nuevas_gerencias_a_registrar, excepciones_gerencia
                )
                
                if len(filtro_unidad_organizativa) == 0:
                    return {'respuesta':nuevas_gerencias_a_registrar,'estado':1} if len(nuevas_gerencias_a_registrar) > 0 else {'respuesta':nuevas_gerencias_a_registrar,'estado':0}
              
            else:
                nuevas_gerencias_a_registrar = []
            
            return {'respuesta':nuevas_gerencias_a_registrar,'estado':1} if len(nuevas_gerencias_a_registrar)>0 else {'respuesta':nuevas_gerencias_a_registrar,'estado':0}

        except Exception as e:
            print(f"Se produjo un error: {str(e)}")
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e


    # Aqui se obtiene los que se pueden actualizar en la gerencia es decir los que han sufrido cambios
    def obtener_gerencias_actualizacion(self):
        
        if self.__validacion_contenido:
            df_gerencia = pd.DataFrame(self.__gerencia)
            df_obtener_unidad_de_gerencia = pd.DataFrame(self.__obtener_gerencia_existente)
            
            df_gerencia['responsable_id'] = df_gerencia['responsable_id'].astype(int)
            
            df_gerencia['nombre'] = df_gerencia['nombre']
            df_obtener_unidad_de_gerencia['nombre'] = df_obtener_unidad_de_gerencia['nombre']
            
            df_gerencia['unidad_gerencia_id_erp'] = df_gerencia['unidad_gerencia_id_erp']
            df_obtener_unidad_de_gerencia['unidad_gerencia_id_erp'] = df_obtener_unidad_de_gerencia['unidad_gerencia_id_erp']
            
            resultado = pd.merge(
                df_gerencia[['unidad_gerencia_id_erp','nombre','responsable_id']],
                df_obtener_unidad_de_gerencia[['id','unidad_gerencia_id_erp','nombre','responsable_id']],
                left_on=['unidad_gerencia_id_erp'],
                right_on=['unidad_gerencia_id_erp'],
                how='inner',
            )
            
            # Seleccionar las columnas deseadas
            
            resultado_final = resultado[['id', 'unidad_gerencia_id_erp', 'nombre_x', 'responsable_id_x']].rename(columns={'nombre_x': 'nombre', 'responsable_id_x': 'responsable_id'})

            
            gerencia_actualizar = resultado_final[
            ~resultado_final.apply(lambda x: (
                (x['nombre'] in set(df_obtener_unidad_de_gerencia['nombre'])) and
                (x['unidad_gerencia_id_erp'] != df_obtener_unidad_de_gerencia.loc[df_obtener_unidad_de_gerencia['nombre'] == x['nombre'], 'unidad_gerencia_id_erp'].values[0])
            ), axis=1)
        ]
            
            if gerencia_actualizar.empty:
                return {'respuesta':[],'estado':0}
            
            actualizacion_gerncia = gerencia_actualizar[['id','nombre','responsable_id']]
            filtrado_actualizacion = actualizacion_gerncia.to_dict(orient = 'records')
            
            filtro_gerencia = self.obtener_no_sufrieron_cambios()['respuesta']
            
            
            if len(filtro_gerencia) != 0:
                df_gerencia = pd.DataFrame(filtro_gerencia)
                columnas_filtrar = ['id','nombre', 'responsable_id']
                df_filtrado = gerencia_actualizar[~gerencia_actualizar[columnas_filtrar].isin(df_gerencia[columnas_filtrar].to_dict('list')).all(axis=1)]
                filtrado_actualizacion = df_filtrado.to_dict(orient = 'records')
                return {'respuesta':filtrado_actualizacion,'estado':2} if len(filtrado_actualizacion) > 0 else {'respuesta':filtrado_actualizacion,'estado':0}
 
        else:
            filtrado_actualizacion = []
        

        return {'respuesta':filtrado_actualizacion,'estado':2} if len(filtrado_actualizacion) > 0 else {'respuesta':filtrado_actualizacion,'estado':0}


    # esto son los que no han tenido nada de cambios pero lo han querido enviar a actualizar
    #  se lleva un registro de estos
    # esta validacion es para los usuarios que tienen items repetidos
    def obtener_no_sufrieron_cambios(self):
 
        if self.__validacion_contenido:
            df_unidad_gerencia = pd.DataFrame(self.__gerencia)
            df_obtener_unidad_gerencia_existentes = pd.DataFrame(self.__obtener_gerencia_existente)
            
            df_unidad_gerencia['nombre'] = df_unidad_gerencia['nombre']
            df_obtener_unidad_gerencia_existentes['nombre'] = df_obtener_unidad_gerencia_existentes['nombre']
            
            no_sufren_cambios = pd.merge(
                df_unidad_gerencia,
                df_obtener_unidad_gerencia_existentes,
                how='inner',
                on=['unidad_gerencia_id_erp','nombre','responsable_id']
            )
            
            gerencias_sin_cambios = no_sufren_cambios.to_dict(orient='records')
        else:
            gerencias_sin_cambios = []

        return {'respuesta':gerencias_sin_cambios,'estado':3} if len(gerencias_sin_cambios) > 0 else {'respuesta':gerencias_sin_cambios,'estado':0}


    # esto son los que no han tenido nada de cambios pero lo han querido enviar a actualizar
    #  se lleva un registro de estos
    # esta validacion es para los usuarios que tienen items repetidos
    def excepciones_id_usuario(self):
        
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
            
        return {'respuesta':id_usuario_no_existe,'estado':4} if len(id_usuario_no_existe) > 0 else {'respuesta':id_usuario_no_existe,'estado':0}


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
                # session.bulk_insert_mappings(ProyectoUnidadGerencia, novedades_de_gerencia)
                return novedades_de_gerencia

            return "No se han registrado datos"
        except SQLAlchemyError as e:
              print(f"Se produjo un error de SQLAlchemy: {e}")
            #   return e
        

    def actualizar_informacion(self, actualizacion_gerencia):
        try:
            if len(actualizacion_gerencia) > 0  :
                # session.bulk_update_mappings(ProyectoUnidadGerencia, actualizacion_gerencia)
                return actualizacion_gerencia

            return "No se han actualizado datos"
        except SQLAlchemyError as e:
              print(f"Se produjo un error de SQLAlchemy: {e}")
            #   return e

    # se realiza un mapeo para realizar el filtro de la gerencia actualizar a registrar
    # y me envia la lista de gerencias que se le va realizar la insercion o la actualizacion
    def gerencias_mapeo_excepciones(self, gerencia, excepciones):
        # filtro de excepciones atrapada de datos unicos, el cual obtiene la informacion nueva y la filtra con las exepciones que existen

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

    
        # obtengo las excepciones del usuario me esta enviando informacion que debe ser unica y la filtro lo que me viene en la bd contra lo que me envia el usuario
    # y le devuelvo la informacion
    def obtener_excepciones_datos_unicos(self):
        validacion = self.__validacion_contenido
        
        if validacion:
            df_gerencia = pd.DataFrame(self.__gerencia)
            df_obtener_unidad_de_gerencia = pd.DataFrame(self.__obtener_gerencia_existente)
            
            df_gerencia['nombre'] = df_gerencia['nombre']
            df_obtener_unidad_de_gerencia['nombre'] = df_obtener_unidad_de_gerencia['nombre']
            
            df_gerencia['unidad_gerencia_id_erp'] = df_gerencia['unidad_gerencia_id_erp']
            df_obtener_unidad_de_gerencia['unidad_gerencia_id_erp'] = df_obtener_unidad_de_gerencia['unidad_gerencia_id_erp']
            
            
            gerencias_existentes = df_gerencia[
                df_gerencia.apply(
                    lambda x: (
                        (
                            (x['nombre'].lower() in df_obtener_unidad_de_gerencia['nombre'].str.lower().values) and
                            (x['unidad_gerencia_id_erp'].lower() != df_obtener_unidad_de_gerencia.loc[df_obtener_unidad_de_gerencia['nombre'].str.lower() == x['nombre'].lower(), 'unidad_gerencia_id_erp'].str.lower().values[0])
                        )
                    ),
                    axis=1
                ) |
                    df_gerencia['unidad_gerencia_id_erp'].str.lower().isin(df_obtener_unidad_de_gerencia['unidad_gerencia_id_erp'].str.lower().values)
                ]
            
            
            actualizar_ = self.obtener_gerencias_actualizacion()['respuesta']
            
            if len(actualizar_) > 0:
                df_cliente = pd.DataFrame(actualizar_)
                df_filtrado = gerencias_existentes[~gerencias_existentes[['nombre','responsable_id']].isin(df_cliente[['nombre','responsable_id']].to_dict('list')).all(axis=1)]
                filtro_cliente_actualizacion =  df_filtrado.to_dict(orient='records')
                return {'respuesta':filtro_cliente_actualizacion,'estado':3} if len(filtro_cliente_actualizacion) > 0 else {'respuesta':filtro_cliente_actualizacion,'estado':0}
            
            gerencia_filtro = self.obtener_no_sufrieron_cambios()['respuesta']
            obtener_excepcion = gerencias_existentes.to_dict(orient='records')
            
            if len(gerencia_filtro) != 0:
                df_cliente = pd.DataFrame(gerencia_filtro)
                df_filtrado = gerencias_existentes[~gerencias_existentes[['unidad_gerencia_id_erp','nombre','responsable_id']].isin(df_cliente[['unidad_gerencia_id_erp','nombre','responsable_id']].to_dict('list')).all(axis=1)]
                filtro_cliente_actualizacion =  df_filtrado.to_dict(orient='records')
                return {'respuesta':filtro_cliente_actualizacion,'estado':3} if len(filtro_cliente_actualizacion) > 0 else {'respuesta':filtro_cliente_actualizacion,'estado':0}
      
        else:
            obtener_excepcion = []
        
        return {'respuesta':obtener_excepcion,'estado':3} if len(obtener_excepcion) > 0 else {'respuesta':obtener_excepcion,'estado':0}