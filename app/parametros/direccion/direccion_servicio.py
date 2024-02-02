from typing import List
from app.funcionalidades_archivos.funciones_archivos_excel import GestorExcel
from app.parametros.direccion.model.proyecto_unidad_organizativa import ProyectoUnidadOrganizativa
from app.database.db import session
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from sqlalchemy import and_,func
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from fastapi import  UploadFile
from sqlalchemy.dialects.postgresql import insert
from app.parametros.mensajes_resultado.mensajes import DireccionMensaje, GlobalMensaje

class Direccion:
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        resultado_estructuracion = self.__proceso_de_informacion_estructuracion()
        self.__informacion_excel_duplicada = resultado_estructuracion
        self.__direcion_excel = resultado_estructuracion['resultado']
        self.__obtener_unidad_organizativa_existentes = self.obtener_direccion()
        self.__unidad_organizativa = self.proceso_informacion_con_id_gerencia()
        self.__validacion_contenido = len(self.__unidad_organizativa) > 0 and len(self.__obtener_unidad_organizativa_existentes) > 0
        
    
    # class
    def __proceso_de_informacion_estructuracion(self):
        df = pd.read_excel(self.__file.file)
        # Imprimir las columnas reales del DataFrame
        df.columns = df.columns.str.strip()
        selected_columns = ["ID Dirección (ERP)", "Dirección", "ID Gerencia (ERP)"]

        df_excel = df[selected_columns]
        
        df_excel = df_excel.dropna()
        
        if df_excel.empty:
                return {'resultado': [], 'duplicados': [],'cantidad_duplicados':0,'estado':0}
        # Cambiar los nombres de las columnas
        df_excel = df_excel.rename(
            columns={
                "ID Dirección (ERP)": "unidad_organizativa_id_erp",
                "Dirección": "nombre",
                "ID Gerencia (ERP)": "unidad_gerencia_id_erp",
            }
        )
        
        df_excel["unidad_organizativa_id_erp"] = df_excel["unidad_organizativa_id_erp"].str.strip()
        df_excel["nombre"] = df_excel["nombre"].str.strip()
        

        duplicados_unidad_erp = df_excel.duplicated(subset='unidad_organizativa_id_erp', keep=False)
        duplicados_nombre = df_excel.duplicated(subset='nombre', keep=False)
   
        # Filtrar DataFrame original
        resultado = df_excel[~(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        duplicados = df_excel[(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')
        
        cantidad_duplicados = len(duplicados)
        
        return {
                    'resultado':resultado,
                    'duplicados':duplicados[0] if cantidad_duplicados > 0 else [],
                    'cantidad_duplicados':cantidad_duplicados,
                    'estado': 3 if cantidad_duplicados > 0 else 0
                }
    
    # class
    def proceso_sacar_estado(self):
            novedades_de_unidad_organizativa = self.comparacion_unidad_organizativa()
            
            unidad_organizativa_update = self.obtener_unidad_organizativa_actualizacion()['estado']
            sin_cambios = novedades_de_unidad_organizativa.get("excepciones_campos_unicos")['estado']
            excepciones_id_usuario = novedades_de_unidad_organizativa.get("exepcion_unidad_organizativa")['estado']
            unidad_organizativas_existentes = novedades_de_unidad_organizativa.get("unidad_organizativas_existentes")['estado']
            estado_duplicado = self.__informacion_excel_duplicada['estado']
            lista_insert = novedades_de_unidad_organizativa.get("insercion_datos")['estado']

            # Crear un conjunto con todos los valores de estado
            estados = {lista_insert, unidad_organizativa_update, sin_cambios, excepciones_id_usuario, unidad_organizativas_existentes,estado_duplicado}

            # Filtrar valores diferentes de 0 y eliminar duplicados
            estados_filtrados = [estado for estado in estados if estado != 0]
            
            return estados_filtrados
    
    
    # class
    def transacciones(self):
        try:
            if len(self.__unidad_organizativa) > 0:
                novedades_de_unidad_organizativa = self.comparacion_unidad_organizativa()
                # informacion a insertar
                lista_insert = novedades_de_unidad_organizativa.get("insercion_datos")['respuesta']
                
                unidad_organizativa_update = self.obtener_unidad_organizativa_actualizacion()['respuesta']
                
                sin_cambios = novedades_de_unidad_organizativa.get("excepciones_campos_unicos")['respuesta']
                
                excepciones_id_usuario = novedades_de_unidad_organizativa.get("exepcion_unidad_organizativa")['respuesta']
                
                unidad_organizativas_existentes = novedades_de_unidad_organizativa.get("unidad_organizativas_existentes")['respuesta']
            
                log_transaccion_registro = self.insertar_informacion(lista_insert)
                
                log_transaccion_actualizar = self.actualizar_informacion(unidad_organizativa_update)
                
                estado_id = self.proceso_sacar_estado()
                
                log_transaccion_registro_unidad_organizativa = {
                        "log_transaccion_excel": {
                            'Respuesta':[
                                {
                                    "direccion_registradas": log_transaccion_registro,
                                    "direccion_actualizadas": log_transaccion_actualizar,
                                    "direccion_sin_cambios": sin_cambios,
                                }
                            ],
                            'errores':{
                                "direccion_id_no_existe": {'datos':excepciones_id_usuario,'mensaje':DireccionMensaje.EXCEPCION_NO_EXISTE.value} if len(excepciones_id_usuario) > 0 else [],
                                "direccion_duplicadas": {'datos':self.__informacion_excel_duplicada['duplicados'] ,'mensaje':GlobalMensaje.mensaje(self.__informacion_excel_duplicada['cantidad_duplicados'])} if len(self.__informacion_excel_duplicada['duplicados']) else [],
                                "direccion_existentes":{'datos':unidad_organizativas_existentes,'mensaje':DireccionMensaje.EXCEPCION_DATOS_UNICOS.value} if len(unidad_organizativas_existentes) > 0 else []
                            }
                        },
                        'estado':{
                            'id':estado_id
                        }
                    }

                return log_transaccion_registro_unidad_organizativa
            
            gestor_excel = GestorExcel()
            
            dato_estado = gestor_excel.transformacion_estados(self.__informacion_excel_duplicada)
            dato_estado.insert(0, 0)
            dato_estado = list(set(dato_estado))
            
            return { 
                    'mensaje': GlobalMensaje.NO_HAY_INFORMACION.value,
                    'duplicados': {'datos':self.__informacion_excel_duplicada['duplicados'] ,'mensaje':GlobalMensaje.mensaje(self.__informacion_excel_duplicada['cantidad_duplicados'])} if len(self.__informacion_excel_duplicada['duplicados']) else [],
                    'estado': dato_estado
                    }

        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()
      
    
    # class
    def obtener_direccion(self):
            informacion_direccion = session.query(ProyectoUnidadOrganizativa).all()
            direccion_datos = [direccion.to_dict() for direccion in informacion_direccion]
            return direccion_datos
        
    
    
    def obtener_por_estado_unidad_organizativa(self, estado=1):
        informacion_direccion = session.query(ProyectoUnidadOrganizativa).filter_by(estado=estado).all()
        # Convertir lista de objetos a lista de diccionarios
        direccion_datos = [direccion.to_dict() for direccion in informacion_direccion]
        return direccion_datos
    
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
                "unidad_organizativas_existentes":excepciones_datos_unicos
            }

            return resultado
        except Exception as e:
            raise (f"Error al realizar la comparación: {str(e)}")
      
    def filtrar_unidad_organizativa(self):
        excepciones_unidad_organizativa = self.obtener_no_sufrieron_cambios()
        unidad_organizativa_id_erp = self.unidad_organizativa_id_erp()
        registro_unidad_organizativa = self.filtrar_unidad_organizativas_nuevas(excepciones_unidad_organizativa)
        excepciones_datos_unicos = self.unidad_organizativas_existentes()

        return (
            excepciones_unidad_organizativa,
            unidad_organizativa_id_erp,
            registro_unidad_organizativa,
            excepciones_datos_unicos
        )
    
    def unidad_organizativa_id_erp(self):
        if len(self.__unidad_organizativa) > 0:
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
        
        return {'respuesta':id_gerencias_none,'estado':3} if len(id_gerencias_none) > 0 else {'respuesta':id_gerencias_none,'estado':0}
    
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

    
    def obtener_gerencia(self, unidad_gerencia_id_erp):
        obtener_gerencia =  (
            session.query(ProyectoUnidadGerencia)
            .filter(
                and_(
                    ProyectoUnidadGerencia.unidad_gerencia_id_erp == unidad_gerencia_id_erp,
                    ProyectoUnidadGerencia.estado == 1,
                )
            )
            .first()
        )
        
        return obtener_gerencia
        
    def filtrar_unidad_organizativas_nuevas(self, excepciones_unidad_organizativa):
        try:
            if len(self.__obtener_unidad_organizativa_existentes) == 0:
                
                df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
                
                resultado = df_unidad_organizativa[(df_unidad_organizativa['gerencia_id'] != 0) ]
                
                registrar_data = resultado.to_dict(orient='records')
                
                return {'respuesta':registrar_data,'estado':1} if len(registrar_data) > 0 else {'respuesta':registrar_data,'estado':0}
            
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
    
    
    def obtener_existe_contenido(self)->bool:
        return len(self.__unidad_organizativa) > 0 and len(self.__obtener_unidad_organizativa_existentes) > 0
    
    def obtener_unidad_organizativa_actualizacion(self):

        unidad_organizativa_activos = self.obtener_existe_contenido()
        
        if unidad_organizativa_activos:
            
                df_unidad_organizativa = pd.DataFrame(self.__unidad_organizativa)
                df_obtener_unidad_organizativa_existentes = pd.DataFrame(self.obtener_por_estado_unidad_organizativa())

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
                
                # si existen exepciones de direccion_id_no_existentes
                filtro_excpeciones = self.unidad_organizativa_id_erp()['respuesta']
                
                columnas_filtrar = ['unidad_organizativa_id_erp', 'nombre']
                
                gestor_proceso_excel = GestorExcel(columnas_filtrar)
                
                filtrar_las_actualizaciones = gestor_proceso_excel.filtro_de_excpeciones(filtro_direccion,filtro_excpeciones,direccion_actualizar)
                
                if len(filtrar_las_actualizaciones['respuesta']) > 0 or filtrar_las_actualizaciones['estado']:
                    return {'respuesta': filtrar_las_actualizaciones['respuesta'], 'estado' : 3 if len(filtrar_las_actualizaciones['respuesta'])>0 else 0}

                filtrado_actualizacion = direccion_culmnas.to_dict(orient='records')
        else:
             filtrado_actualizacion = []
             
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
            cantidad_unidad_organizativa_registradas = len(novedades_unidad_organizativa)
            if cantidad_unidad_organizativa_registradas > 0:
                # insertar_informacion = insert(ProyectoUnidadOrganizativa,novedades_unidad_organizativa)
                # session.execute(insertar_informacion)
                # session.commit()
                return {'mensaje': f'Se han realizado {cantidad_unidad_organizativa_registradas} registros exitosos.' if cantidad_unidad_organizativa_registradas > 1 else  f'Se ha registrado una ({cantidad_unidad_organizativa_registradas}) Unidad Organizativa exitosamente.'}
            return "No se han registrado datos"
        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        try:
            cantidad_unidad_organizativa_actualizadas = len(actualizacion_gerencia_unidad_organizativa)
            if cantidad_unidad_organizativa_actualizadas > 0:
                # session.bulk_update_mappings(ProyectoUnidadOrganizativa, actualizacion_gerencia_unidad_organizativa)
                # session.commit()
                return {'mensaje': f'Se han actualizado {cantidad_unidad_organizativa_actualizadas} Unidad Organizativa exitosamente.' if cantidad_unidad_organizativa_actualizadas > 1 else  f'Se ha registrado una ({cantidad_unidad_organizativa_actualizadas}) Unidad Organizativa exitosamente.'}

            return "No se han actualizado datos"
        except SQLAlchemyError as e:
            session.rollback()
            raise (e)
        finally:
            session.close()
    
    def proceso_informacion_con_id_gerencia(self):
        try:
            resultados = [
                {
                    "unidad_organizativa_id_erp": unidad_organizativa["unidad_organizativa_id_erp"],
                    "nombre": unidad_organizativa["nombre"],
                    "gerencia_id": self.obtener_gerencia(unidad_organizativa["unidad_gerencia_id_erp"]).to_dict().get("id") if self.obtener_gerencia(unidad_organizativa["unidad_gerencia_id_erp"]) else 0,
                }
                for unidad_organizativa in self.__direcion_excel
            ]
            
            return resultados
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e
        
        
    
    def unidad_organizativas_existentes(self):
        
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
            
            direccion_filtro_no_sufrieron_cambios = self.obtener_no_sufrieron_cambios()['respuesta']
            
            actualizar_ = self.obtener_unidad_organizativa_actualizacion()['respuesta']
      
            filtrar_las_actualizaciones = self.filtro_de_excepciones(direccion_filtro_no_sufrieron_cambios,actualizar_,direcciones_existentes)
            respuesta_filtro = filtrar_las_actualizaciones['respuesta']
            
            if len(respuesta_filtro) > 0 or filtrar_las_actualizaciones['estado'] == 3:
                return  {'respuesta': respuesta_filtro, 'estado': 3 if len(respuesta_filtro) > 0 else 0}
            
            obtener_excepcion = direcciones_existentes.to_dict(orient='records')
        else:
            obtener_excepcion = []
        return {'respuesta':obtener_excepcion,'estado':3} if len(obtener_excepcion) > 0 else {'respuesta':obtener_excepcion,'estado':0}
    
    
    
    def filtro_de_excepciones(self,direccion_filtro,filtro_actualizacion,direccion_excepcion:pd.DataFrame):

            if len(filtro_actualizacion) > 0 and len(direccion_filtro) > 0:
                
                    df_direccion_filtro_actualizacion = pd.DataFrame(filtro_actualizacion)
                    df_gerencia = pd.DataFrame(direccion_filtro)
                    
                    df_filtrado = direccion_excepcion[
                        ~direccion_excepcion[['nombre','gerencia_id']].isin(df_direccion_filtro_actualizacion[['nombre','gerencia_id']].to_dict('list')).all(axis=1) &
                        ~direccion_excepcion[['unidad_organizativa_id_erp','nombre','gerencia_id']].isin(df_gerencia[['unidad_organizativa_id_erp','nombre','gerencia_id']].to_dict('list')).all(axis=1)
                    ]
            
                    filtro_combinado = df_filtrado.to_dict(orient='records')
                    return {'respuesta': filtro_combinado, 'estado': 3} 
            
           
            elif len(filtro_actualizacion) > 0:
                df_direccion_filtro_actualizacion = pd.DataFrame(filtro_actualizacion)
                df_filtrado = direccion_excepcion[~direccion_excepcion[['nombre','gerencia_id']].isin(df_direccion_filtro_actualizacion[['nombre','gerencia_id']].to_dict('list')).all(axis=1)]
                filtro_direccion =  df_filtrado.to_dict(orient ='records')
                return {'respuesta':filtro_direccion,'estado':3} 
            
            
            elif len(direccion_filtro) > 0:
                df_direccion = pd.DataFrame(direccion_filtro)
                df_filtrado = direccion_excepcion[~direccion_excepcion[['unidad_organizativa_id_erp','nombre','gerencia_id']].isin(df_direccion[['unidad_organizativa_id_erp','nombre','gerencia_id']].to_dict('list')).all(axis=1)]
                filtro_direccion =  df_filtrado.to_dict(orient = 'records')
                
                return {'respuesta':filtro_direccion,'estado':3} 
            else:
                return {'respuesta': [], 'estado': 0}