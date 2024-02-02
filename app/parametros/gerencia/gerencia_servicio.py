import math
from app.funcionalidades_archivos.funciones_archivos_excel import GestorExcel
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from app.database.db import session
from typing import List
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
from fastapi import  UploadFile
import pandas as pd
from app.parametros.mensajes_resultado.mensajes import MensajeAleratGerenica,GlobalMensaje
from sqlalchemy.dialects.postgresql import insert

class Gerencia:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        resultado_estructuracion = self.__proceso_de_informacion_estructuracion()
        self.__informacion_excel_duplicada = resultado_estructuracion
        self.__gerencia_excel = resultado_estructuracion['resultado']
        # todas las gerencias existentes en la base de datos
        self.__obtener_gerencia_existente = self.obtener()
        self.__obtener_gerencia_existente_estado = self.obtener_por_estado_gerencia()
        # gerencia que me envia el usuario a traves del excel
        self.__gerencia = self.gerencia_usuario_procesada()
        self.__validacion_contenido = len(self.__gerencia) > 0 and len(self.__obtener_gerencia_existente) > 0
        self.__validacion_contenido_estado = len(self.__gerencia) > 0 and len(self.__obtener_gerencia_existente_estado) > 0
    
    # class
    def __proceso_de_informacion_estructuracion(self):
        df = pd.read_excel(self.__file.file)
        
        # Imprimir las columnas reales del DataFrame
        df.columns = df.columns.str.strip()
        
        selected_columns = ["ID Gerencia (ERP)", "Gerencia", "Responsable"]

        df_excel = df[selected_columns]
        
        df_excel = df_excel.dropna()
        
        if df_excel.empty:
                return {'resultado': [], 'duplicados': [],'cantidad_duplicados':0,'estado': 0}

        # Cambiar los nombres de las columnas
        df_excel = df_excel.rename(
            columns={
               "ID Gerencia (ERP)": "unidad_gerencia_id_erp", 
               "Gerencia": "nombre", 
                "Responsable": "NIT"
            }
        )
        
        # Convertir la columna "unidad_gerencia_id_erp" a tipo string y eliminar espacios
        df_excel["unidad_gerencia_id_erp"] = df_excel["unidad_gerencia_id_erp"].astype(str).str.strip()

        # Convertir la columna "nombre" a tipo string y eliminar espacios
        df_excel["nombre"] = df_excel["nombre"].astype(str).str.strip()
        
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
        
        cantidad_duplicados = len(lista_gerencias)
        return {
                'resultado':resultado,
                'duplicados':lista_gerencias[0] if cantidad_duplicados > 0 else [],
                'cantidad_duplicados':cantidad_duplicados,
                'estado': 3 if cantidad_duplicados > 0 else 0 
                }
        
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
                    
            return {'log': gerencia_log, 'gerencia_filtrada_excel': gerencia_filtrada_excel,'estado': 3 if len(gerencia_log) > 0 else 0}

        except Exception as e:
            raise Exception(f"Error al realizar la comparación: {str(e)}") from e

    
    # class
    def obtener(self):
        informacion_gerencia = session.query(ProyectoUnidadGerencia).all()
        # Convertir lista de objetos a lista de diccionarios
        gerencia_data = [gerencia.to_dict() for gerencia in informacion_gerencia]
        return gerencia_data
    
    def obtener_por_estado_gerencia(self, estado=1):
        informacion_gerencia = session.query(ProyectoUnidadGerencia).filter_by(estado=estado).all()
        # Convertir lista de objetos a lista de diccionarios
        gerencia_data = [gerencia.to_dict() for gerencia in informacion_gerencia]
        return gerencia_data

    # class
    def proceso_sacar_estado(self):
            novedades_de_gerencia = self.comparacion_gerencia()
            
            lista_insert = novedades_de_gerencia.get("insercion_datos")['estado']
            
            gerencia_update = novedades_de_gerencia.get("actualizacion_datos")['estado']
            
            sin_cambios = novedades_de_gerencia.get("excepciones_campos_unicos")['estado']
            
            excepciones_id_usuario = novedades_de_gerencia.get("exepcion_id_usuario")['estado']
            
            excepciones_gerencia = novedades_de_gerencia.get("excepcion_gerencia_existentes")['estado']
            
            log_nit_invalido = self.validacion_informacion_gerencia_nit()['estado']
            
            duplicados_estado = self.__informacion_excel_duplicada['estado']
            
            # Crear un conjunto con todos los valores de estado
            estados = {lista_insert, gerencia_update, sin_cambios, excepciones_id_usuario, 
                       excepciones_gerencia, log_nit_invalido,duplicados_estado}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados if len(estados_filtrados) > 0 else [0]  
    
    # class
    def transacciones(self):
        try:
            log_nit_invalido = self.validacion_informacion_gerencia_nit()
            estado_id = self.proceso_sacar_estado()
            
            if  len(self.__gerencia) > 0:
                novedades_de_gerencia = self.comparacion_gerencia()
                # informacion a insertar
                lista_insert = novedades_de_gerencia.get("insercion_datos")['respuesta']
                gerencia_update = novedades_de_gerencia.get("actualizacion_datos")['respuesta']
                sin_cambios = novedades_de_gerencia.get("excepciones_campos_unicos")['respuesta']
                excepciones_id_usuario = novedades_de_gerencia.get("exepcion_id_usuario")['respuesta']
                excepciones_gerencia = novedades_de_gerencia.get("excepcion_gerencia_existentes")['respuesta']
                log_transaccion_registro = self.insertar_inforacion(lista_insert)
                log_transaccion_actualizar = self.actualizar_informacion(gerencia_update)

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
                        "gerencia_filtro_nit_invalido":{'datos':log_nit_invalido['log'] , 'mensaje':GlobalMensaje.NIT_INVALIDO.value} if len(log_nit_invalido['log']) else [],
                        "gerencias_existentes":excepciones_gerencia,
                        "gerencias_duplicadas": {'datos':self.__informacion_excel_duplicada['duplicados'] , 'mensaje':MensajeAleratGerenica.mensaje(self.__informacion_excel_duplicada['cantidad_duplicados'])} 
                                                    if len(self.__informacion_excel_duplicada['duplicados']) else []
                        }
            
                    },
                    'estado':{
                        'id':estado_id
                    }
                }

                return log_transaccion_registro_gerencia
            
            
            dato_estado = estado_id
            dato_estado.insert(0, 0)
            dato_estado = list(set(dato_estado))
            
            return { 
                    'mensaje':GlobalMensaje.NO_HAY_INFORMACION.value,
                    'nit_invalidos':{'datos':log_nit_invalido['log'],'mensaje':GlobalMensaje.NIT_INVALIDO.value} if len(log_nit_invalido['log']) else [],
                    'duplicados': {'datos':self.__informacion_excel_duplicada['duplicados'] ,'mensaje':MensajeAleratGerenica.mensaje(self.__informacion_excel_duplicada['cantidad_duplicados'])} if len(self.__informacion_excel_duplicada['duplicados']) else [],
                    'estado':dato_estado 
                    }

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
    
    # class
    def filtrar_gerencias_nuevas(self, excepciones_gerencia):
        try:
            if len(self.__obtener_gerencia_existente) == 0:
                
                df_unidad_gerencia = pd.DataFrame(self.__gerencia)
                
                resultado = df_unidad_gerencia[(df_unidad_gerencia['responsable_id'] != 0) ]
                
                registrar_data = resultado.to_dict(orient='records')
                
                return {'respuesta':registrar_data,'estado':1} if len(registrar_data) > 0 else {'respuesta':registrar_data,'estado':0}
            
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
            
            return {'respuesta': nuevas_gerencias_a_registrar,'estado':1} if len(nuevas_gerencias_a_registrar)>0 else {'respuesta':nuevas_gerencias_a_registrar,'estado':0}

        except Exception as e:
            print(f"Se produjo un error: {str(e)}")
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e


    # Aqui se obtiene los que se pueden actualizar en la gerencia es decir los que han sufrido cambios
    
    # class
    def obtener_gerencias_actualizacion(self):
        
        if self.__validacion_contenido_estado:
            df_gerencia = pd.DataFrame(self.__gerencia)
            df_obtener_unidad_de_gerencia = pd.DataFrame(self.__obtener_gerencia_existente_estado)
            
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

            filtro_de_id_gerencia = resultado_final[resultado_final['responsable_id'] != 0]
            
            gerencia_actualizar = filtro_de_id_gerencia[
            ~filtro_de_id_gerencia.apply(lambda x: (
                (x['nombre'].lower() in set(df_obtener_unidad_de_gerencia['nombre'].str.lower())) and
                (x['unidad_gerencia_id_erp'] != df_obtener_unidad_de_gerencia.loc[df_obtener_unidad_de_gerencia['nombre'].str.lower() == x['nombre'].lower(), 'unidad_gerencia_id_erp'].values[0]) and
                (x['responsable_id'] != 0)
            ), axis=1)
        ]
            if gerencia_actualizar.empty:
                return {'respuesta':[],'estado':0}
            
            actualizacion_gerncia = gerencia_actualizar[['id','nombre','responsable_id']]
            filtrado_actualizacion = actualizacion_gerncia.to_dict(orient = 'records')
            
            filtro_gerencia = self.obtener_no_sufrieron_cambios()['respuesta']
            
            
            if len(filtro_gerencia) != 0:
                df_gerencia = pd.DataFrame(filtro_gerencia)
                columnas_filtrar = ['nombre', 'responsable_id']
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
        
        if len(self.__gerencia) > 0:
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
        
        cantidad_excepciones_id_usuario = len(id_usuario_no_existe)
        return {'respuesta':{'datos':id_usuario_no_existe,'mensaje':GlobalMensaje.NIT_NO_ENCONTRADO.value} if cantidad_excepciones_id_usuario else []
                ,'estado':3 if cantidad_excepciones_id_usuario > 0 else 0}


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
                    usuario = self.encontrar_id_usuario(int(nit_value))['id_usuario']

                    resultados.append({
                        "unidad_gerencia_id_erp": gerencia["unidad_gerencia_id_erp"],
                        "nombre": gerencia["nombre"],
                        "responsable_id": usuario if usuario else 0,
                    })
            return resultados

        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e
        
    def encontrar_id_usuario(self, identificacion):
        gestor_excel = GestorExcel()
        id_gerente_encargado = gestor_excel.obtener_id_usuario(identificacion) 
        return id_gerente_encargado

    def insertar_inforacion(self, novedades_de_gerencia: List):
        try:
            cantidad_gerencias_registradas = len(novedades_de_gerencia)
            if cantidad_gerencias_registradas > 0:
                # insertar_informacion = insert(ProyectoUnidadGerencia,novedades_de_gerencia)
                # session.execute(insertar_informacion)
                # session.commit()
                return {'mensaje': f'Se han realizado {cantidad_gerencias_registradas} registros exitosos.' if cantidad_gerencias_registradas > 1 else  f'Se ha registrado una ({cantidad_gerencias_registradas}) proyecto Estado exitosamente.'}

            return "No se han registrado datos"
        except SQLAlchemyError as e:
              print(f"Se produjo un error de SQLAlchemy: {e}")
            #   return e
        

    def actualizar_informacion(self, actualizacion_gerencia):
        try:
            cantidad_clientes_actualizadas = len(actualizacion_gerencia)
            if cantidad_clientes_actualizadas > 0  :
                # session.bulk_update_mappings(ProyectoUnidadGerencia, actualizacion_gerencia)
                # session.commit()
                return {'mensaje': f'Se han actualizado {cantidad_clientes_actualizadas} gerencias exitosamente.' if cantidad_clientes_actualizadas > 1 else  f'Se ha actualizado una ({cantidad_clientes_actualizadas}) Gerencia exitosamente.'}

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
            
            gerencia_filtro = self.obtener_no_sufrieron_cambios()['respuesta']
            actualizar_ = self.obtener_gerencias_actualizacion()['respuesta']
            excepcion_nit_usuario_invalido = self.excepciones_id_usuario()['respuesta']
            
            filtrar_las_actualizaciones = self.filtro_de_excepciones(gerencia_filtro,actualizar_,excepcion_nit_usuario_invalido,gerencias_existentes)
            respuesta_filtro = filtrar_las_actualizaciones['respuesta']
            
            if len(respuesta_filtro) > 0 or filtrar_las_actualizaciones['estado'] == 3:
                return  {
                          'respuesta':{
                                        'datos':respuesta_filtro,
                                        'mensaje':MensajeAleratGerenica.GERENCIA_EXCEPCION.value
                                        } if len(respuesta_filtro) else []
                         ,'estado': 3 if len(respuesta_filtro) > 0 else 0
                         }
            
            obtener_excepcion = gerencias_existentes.to_dict(orient='records')
        else:
            obtener_excepcion = []
        
        obtener_cantidad_excepciones_gerencia = len(obtener_excepcion)
        
        return {'respuesta':{'datos':obtener_excepcion, 'mensaje' : MensajeAleratGerenica.GERENCIA_EXCEPCION.value} if obtener_cantidad_excepciones_gerencia else [],
                'estado':3} if obtener_cantidad_excepciones_gerencia > 0 else {'respuesta':obtener_excepcion,'estado':0}
    
    
    def filtro_de_excepciones(self,gerencia_filtro,filtro_actualizacion,filtro_gerencia_nit_no_existe,gerencias_excepciones:pd.DataFrame):
        
            if len(filtro_actualizacion) > 0 and len(gerencia_filtro) > 0 and len(filtro_gerencia_nit_no_existe) > 0:
                    df_gerencia_filtro_actualizacion = pd.DataFrame(filtro_actualizacion)
                    df_gerencia_filtro_gerencia_nit_no_existe = pd.DataFrame(filtro_gerencia_nit_no_existe['datos'])
                    df_gerencia = pd.DataFrame(gerencia_filtro)
                    
                    df_filtrado = gerencias_excepciones[
                        ~gerencias_excepciones[['nombre','responsable_id']].isin(df_gerencia_filtro_actualizacion[['nombre','responsable_id']].to_dict('list')).all(axis=1) &
                        ~gerencias_excepciones[['unidad_gerencia_id_erp','nombre','responsable_id']].isin(df_gerencia[['unidad_gerencia_id_erp','nombre','responsable_id']].to_dict('list')).all(axis=1) &
                        ~gerencias_excepciones[['unidad_gerencia_id_erp','nombre']].isin(df_gerencia_filtro_gerencia_nit_no_existe[['unidad_gerencia_id_erp','nombre']].to_dict('list')).all(axis=1)
                    ]
            
                    filtro_combinado = df_filtrado.to_dict(orient='records')
                    return {'respuesta': filtro_combinado, 'estado': 3} 
                
            elif len(filtro_actualizacion) > 0  and len(filtro_gerencia_nit_no_existe) > 0:
                    df_gerencia_filtro_actualizacion = pd.DataFrame(filtro_actualizacion)
                    df_gerencia_filtro_gerencia_nit_no_existe = pd.DataFrame(filtro_gerencia_nit_no_existe['datos'])
                    df_gerencia = pd.DataFrame(gerencia_filtro)
                    
                    df_filtrado = gerencias_excepciones[
                        ~gerencias_excepciones[['nombre','responsable_id']].isin(df_gerencia_filtro_actualizacion[['nombre','responsable_id']].to_dict('list')).all(axis=1) &
                        ~gerencias_excepciones[['unidad_gerencia_id_erp','nombre']].isin(df_gerencia_filtro_gerencia_nit_no_existe[['unidad_gerencia_id_erp','nombre']].to_dict('list')).all(axis=1)
                    ]
            
                    filtro_combinado = df_filtrado.to_dict(orient='records')
                    return {'respuesta': filtro_combinado, 'estado': 3} 
                
            elif len(filtro_gerencia_nit_no_existe)>0:
                 df_gerencia_filtro_gerencia_nit_no_existe = pd.DataFrame(filtro_gerencia_nit_no_existe['datos'])
                 df_filtro_nit = gerencias_excepciones[~gerencias_excepciones[['unidad_gerencia_id_erp','nombre']].isin(df_gerencia_filtro_gerencia_nit_no_existe[['unidad_gerencia_id_erp','nombre']].to_dict('list')).all(axis=1)]
                 filtro_nit =  df_filtro_nit.to_dict(orient ='records')
                 return {'respuesta':filtro_nit,'estado':3}
           
            elif len(filtro_actualizacion) > 0:
                df_gerencia_filtro_actualizacion = pd.DataFrame(filtro_actualizacion)
                df_filtrado = gerencias_excepciones[~gerencias_excepciones[['nombre','responsable_id']].isin(df_gerencia_filtro_actualizacion[['nombre','responsable_id']].to_dict('list')).all(axis=1)]
                filtro_ceco =  df_filtrado.to_dict(orient ='records')
                return {'respuesta':filtro_ceco,'estado':3} 
            
            
            elif len(gerencia_filtro) > 0:
                df_estado = pd.DataFrame(gerencia_filtro)
                df_filtrado = gerencias_excepciones[~gerencias_excepciones[['unidad_gerencia_id_erp','nombre','responsable_id']].isin(df_estado[['unidad_gerencia_id_erp','nombre','responsable_id']].to_dict('list')).all(axis=1)]
                filtro_estado_actualizacion =  df_filtrado.to_dict(orient = 'records')
                return {'respuesta':filtro_estado_actualizacion,'estado':3}
        
            else:
                return {'respuesta': [], 'estado': 0}