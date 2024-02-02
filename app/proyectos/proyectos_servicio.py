from app.proyectos.model.proyectos import ModeloProyectos
from app.parametros.ceco.model.ceco_model import ProyectoCeco
from app.parametros.estado.model.estado_model import ProyectoEstado
from app.parametros.cliente.model.cliente_model import ProyectoCliente
from app.parametros.direccion.model.proyecto_unidad_organizativa import ProyectoUnidadOrganizativa
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from app.database.db import session
from typing import List
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
from fastapi import  UploadFile
import pandas as pd
from sqlalchemy.orm import aliased
from datetime import datetime
from app.funcionalidades_archivos.funciones_archivos_excel import GestorExcel
from app.parametros.mensajes_resultado.mensajes import GlobalMensaje, MensajeAleratGerenica,ProyectosMensaje
from sqlalchemy.dialects.postgresql import insert

class Proyectos:
    
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        # con esta data se hara la comparacion para los datos que estan en registros
        self.proyectos_existentes = self.obtener()
        # y esta data se realizara la comparacion contra los que tiene para actualizar
        self.proyectos_existentes_por_estado = self.obtener_por_estado_proyectos()
        resultado_estructuracion = self.__proceso_de_proyectos_estructuracion()
        self.__proyectos_excel_duplicada = resultado_estructuracion
        self.__proyectos_excel = resultado_estructuracion['resultado']
        # data procesada contiene los datos en 0 de las id que no existen en otras tablas
        self.__proyectos = self.inforamcion_proyectos_procesada()
        
        
    
    def comparacion_existe_datos(self,obtener_proyectos_existentes)->bool:
        return len(self.__proyectos) > 0 and len(obtener_proyectos_existentes) > 0
    
    def existen_proyectos_excel(self):
        return len(self.__proyectos) > 0
    
    def obtener_excepciones_proyectos(self):
        return {
            "proyecto_responsable_no_existe": self.obtener_exepcion_responsable_no_existe()['respuesta'],
            "proyectos_unidad_administrativas": self.proyectos_unidad_excepcion()['respuesta'],
            "proyectos_estado_no_existe": self.estado_proyecto_no_existe()['respuesta'],
            "proyectos_cliente_no_existe": self.cliente_no_existe()['respuesta'],
            "proyectos_invalidos": self.id_proyectos_invalidos()['respuesta'],
            "fecha_invalidas": self.obtener_fechas_invalidas()['respuesta'],
            "monto_invalidas": self.obtener_montos_invalidas()['respuesta'],
            "proyecto_filtro_nit_invalido": self.validacion_informacion_identificacion()['nit_invalido'],
            "proyectos_duplicadas": {'datos':self.__proyectos_excel_duplicada['duplicados'] ,'mensaje':MensajeAleratGerenica.mensaje(self.__proyectos_excel_duplicada['cantidad_duplicados'])} if len(self.__proyectos_excel_duplicada['duplicados']) else [],
        }
    
    
    def proceso_sacar_estado(self):
        lista_insert = self.obtener_proyectos_registro()['estado']
        gerencia_update = self.obtener_proyectos_actualizacion()['estado']
        estado_cambios = self.obtener_no_sufrieron_cambios()['estado']
        proyecto_responsable_no_existe =  self.obtener_exepcion_responsable_no_existe()['estado']
        proyectos_unidad_administrativas = self.proyectos_unidad_excepcion()['estado']
        proyectos_estado_no_existe = self.estado_proyecto_no_existe()['estado']
        proyectos_cliente_no_existe = self.cliente_no_existe()['estado']
        proyectos_invalidos = self.id_proyectos_invalidos()['estado']
        fecha_invalida = self.obtener_fechas_invalidas()['estado']
        monto_invalidas = self.obtener_montos_invalidas()['estado']
        nit_invalido = self.validacion_informacion_identificacion()['estado']
        proyectos_duplicados = self.__proyectos_excel_duplicada['estado']
        
        # Crear un conjunto con todos los valores de estado
        estados = {lista_insert,gerencia_update,estado_cambios,
                   proyecto_responsable_no_existe, proyectos_unidad_administrativas, 
                   proyectos_estado_no_existe,
                   proyectos_cliente_no_existe, 
                   proyectos_invalidos, fecha_invalida,monto_invalidas,nit_invalido,
                   proyectos_duplicados
                   }

            # Filtrar valores diferentes de 0 y eliminar duplicados
        estados_filtrados = [estado for estado in estados if estado != 0]
            
        return estados_filtrados if len(estados_filtrados)>0 else [0]
    
    def transacciones(self):
        id_estado = self.proceso_sacar_estado()
        # validacion_contenido = self.comparacion_existe_datos(self.proyectos_existentes)

        if self.existen_proyectos_excel():
            
            lista_insert = self.obtener_proyectos_registro()
            gerencia_update = self.obtener_proyectos_actualizacion()
            
            transaccion_registro = self.insertar_inforacion(lista_insert)
            transaccion_actualizar = self.actualizar_informacion(gerencia_update)
            
            log_transaccion_registro_proyecto = {
                        "log_transaccion_excel": {
                            'Respuesta':[
                                {
                                    "proyecto_registradas": transaccion_registro,
                                    "proyectos_actualizadas": transaccion_actualizar,
                                    "proyectos_sin_cambios": self.obtener_no_sufrieron_cambios()['respuesta'],
                                }
                            ],
                            'errores':self.obtener_excepciones_proyectos()
                        },
                        'estado':{
                            'id':id_estado
                        }
                    }
        
            return log_transaccion_registro_proyecto
        
        gestor_excel = GestorExcel()
        
        dato_estado = gestor_excel.transformacion_estados(self.__proyectos_excel_duplicada)
        dato_estado.insert(0, 0)
        dato_estado = list(set(dato_estado))
        return  { 
                    'mensaje':GlobalMensaje.NO_HAY_INFORMACION.value,
                    'duplicados':{'datos':self.__proyectos_excel_duplicada['duplicados'] ,'mensaje':MensajeAleratGerenica.mensaje(self.__proyectos_excel_duplicada['cantidad_duplicados'])} if len(self.__proyectos_excel_duplicada['duplicados']) else [],
                    'estado':id_estado 
                }
    
    def obtener(self):
        datos_proyectos = session.query(ModeloProyectos).all()
        # Convertir lista de objetos a lista de diccionarios
        proyecto_datos = [
        {**proyecto.to_dict(), 
         'fecha_inicio': proyecto.fecha_inicio.strftime('%Y-%m-%d') if proyecto.fecha_inicio else 0,
         'fecha_final': proyecto.fecha_final.strftime('%Y-%m-%d') if proyecto.fecha_final else 0}
        for proyecto in datos_proyectos
        ]
        return proyecto_datos
    
    
    def obtener_por_estado_proyectos(self, estado=1):
        datos_proyectos = session.query(ModeloProyectos).filter_by(estado=estado).all()
        # Convertir lista de objetos a lista de diccionarios
        proyecto_datos = [
        {**proyecto.to_dict(), 
         'fecha_inicio': proyecto.fecha_inicio.strftime('%Y-%m-%d') if proyecto.fecha_inicio else 0,
         'fecha_final': proyecto.fecha_final.strftime('%Y-%m-%d') if proyecto.fecha_final else 0}
        for proyecto in datos_proyectos
        ]
        return proyecto_datos
    
    def __proceso_de_proyectos_estructuracion(self):
        
        df = pd.read_excel(self.__file.file)
        # Imprimir las columnas reales del DataFrame
        df.columns = df.columns.str.strip()
        
        selected_columns = ["ID proyecto",
                            "Nombre del proyecto", 
                            "N° Contrato",
                            "Objeto",
                            "ID Estado (ERP)",
                            "ID Cliente (ERP)",
                            "ID Gerencia (ERP)",
                            "ID Dirección (ERP)",
                            "Gerente",
                            "Fecha inicio",
                            "Fecha fin",
                            "Valor inicial",
                            "Valor final"
                            ]
        

        df_excel = df[selected_columns]
        
        df_excel = df_excel.dropna()
        
        if df_excel.empty:
                return {'resultado': [], 'duplicados': [],'estado':0}

        # Cambiar los nombres de las columnas
        df_excel = df_excel.rename(
            columns={
               "ID proyecto":"ceco_id",
                "Nombre del proyecto":"nombre", 
                "N° Contrato":"contrato",
                "Objeto":"objeto",
                "ID Estado (ERP)":"estado_id",
                "ID Cliente (ERP)":"cliente_id",
                "ID Gerencia (ERP)":"unidad_gerencia_id",
                "ID Dirección (ERP)":"unidad_organizativa_id",
                "Gerente":"identificacion",
                "Fecha inicio":"fecha_inicio",
                "Fecha fin":"fecha_final",
                "Valor inicial":"valor_inicial",
                "Valor final":"valor_final"
            }
        )
        
        df_excel["ceco_id"] = df_excel["ceco_id"]
        df_excel["nombre"] = df_excel["nombre"]
        
        
        df_excel["fecha_inicio"] = df_excel["fecha_inicio"]
        df_excel["fecha_final"] = df_excel["fecha_final"]
        


        duplicados_id_proyecto_erp = df_excel.duplicated(subset='ceco_id', keep=False)
        # Filtrar DataFrame original
        resultado = df_excel[~(duplicados_id_proyecto_erp)].to_dict(orient='records')
        
        duplicados = df_excel[(duplicados_id_proyecto_erp)].to_dict(orient='records')
        
        duplicated = pd.DataFrame(duplicados)
        
        if duplicated.isnull().any(axis=1).any():
            proyectos_lista = []
        else:
            proyectos_lista = [
                {
                    'ceco_id': item['ceco_id'],
                }
                for item in duplicados
            ]
            
        proyectos_duplicados = len(proyectos_lista) 
        
        return {
                'resultado':resultado,
                'duplicados':proyectos_lista[0] if proyectos_duplicados > 0 else [],
                'cantidad_duplicados':proyectos_duplicados,
                'estado':3 if proyectos_duplicados > 0 else 0
                }
    
    
    def validacion_informacion_identificacion(self):
        try:
            if not self.__proyectos_excel:
                return {'nit_invalido': [], 'proyecto_filtro_datos': [],'estado':0}

            proyecto_identificacion_incorrecta, proyecto_filtro_datos = [], []
            
            for item in self.__proyectos_excel:
                if isinstance(item.get('identificacion'), (int, float)):
                    proyecto_filtro_datos.append(item)
                else:
                    proyecto_identificacion_incorrecta.append({
                        'identificacion':item['identificacion'],
                        'id_proyecto':item['ceco_id']
                        })
                    
            return {
                    'nit_invalido': {'datos':proyecto_identificacion_incorrecta,'mensaje':GlobalMensaje.NIT_INVALIDO.value} if len(proyecto_identificacion_incorrecta) > 0 else [] ,
                    'proyecto_filtro_datos': proyecto_filtro_datos,'estado':3 if len(proyecto_identificacion_incorrecta) > 0 else 0
                    }

        except Exception as e:
            raise Exception(f"Error al realizar la comparación: {str(e)}") from e
    
    def inforamcion_proyectos_procesada(self):
        try:
            proyectos_excel = self.validacion_informacion_identificacion()
            resultados = [self.procesar_proyecto(proyecto) for proyecto in proyectos_excel['proyecto_filtro_datos']]
            return resultados

        except Exception as e:
            print(e)
            session.rollback()
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e
    
    def comparacion_columnas_filtro(self,resultado_filtro_actualizacion:pd.DataFrame,df_obtener_proyectos_existentes_columnas_requeridas:pd.DataFrame,columnas_comparar):
            filtro_personalizado = resultado_filtro_actualizacion[
                (resultado_filtro_actualizacion[['ceco_id', 'responsable_id', 'unidad_organizativa_id', 'unidad_gerencia_id', 'cliente_id', 'estado_id', 'fecha_inicio', 'fecha_final', 'valor_inicial', 'valor_final']] != 0).all(axis=1)
            ]
            filtro_personalizado_subset = filtro_personalizado[columnas_comparar]
            df_obtener_proyectos_existentes_columnas_requeridas_subset = df_obtener_proyectos_existentes_columnas_requeridas[columnas_comparar]
            
            return filtro_personalizado_subset,df_obtener_proyectos_existentes_columnas_requeridas_subset
        
    # metodos para obtener las funcionalidades (log de transaccion)
    def obtener_proyectos_actualizacion(self):
        validacion_contenido = self.comparacion_existe_datos(self.proyectos_existentes_por_estado)

        if validacion_contenido:
            
            df_proyectos = pd.DataFrame(self.__proyectos)
            df_obtener_proyectos_existentes = pd.DataFrame(self.proyectos_existentes_por_estado)
            
            df_obtener_proyectos_existentes_columnas_requeridas = df_obtener_proyectos_existentes.drop('proyecto_id_erp', axis=1)
            
            filtro_actualizacion = pd.merge(
                df_proyectos,
                df_obtener_proyectos_existentes_columnas_requeridas,
                on=['ceco_id'],
                how ='inner'
            )
            
            resultado_filtro_actualizacion = filtro_actualizacion[['id','proyecto_id_erp', 'ceco_id', 'nombre_x', 'objeto_x', 'contrato_x', 'responsable_id_x',
                                                       'unidad_organizativa_id_x', 'unidad_gerencia_id_x', 'cliente_id_x',
                                                       'estado_id_x', 'fecha_inicio_x', 'fecha_final_x', 'duracion_x',
                                                       'valor_inicial_x','valor_final_x']].rename(
                columns=lambda x: x.replace('_x', '') if x.endswith('_x') else x
            )
                                                       
                                                       
            columnas_comparar = ['id','nombre', 'responsable_id','fecha_inicio', 'fecha_final', 'valor_inicial', 'valor_final', 'duracion', 'contrato', 'estado_id', 'objeto', 'unidad_gerencia_id', 'unidad_organizativa_id', 'cliente_id', 'ceco_id']                                          
            
            filtro_personalizado_subset,df_obtener_proyectos_existentes_columnas_requeridas_subset = self.comparacion_columnas_filtro(resultado_filtro_actualizacion,df_obtener_proyectos_existentes,columnas_comparar)
            
            resultado_final = filtro_personalizado_subset[~filtro_personalizado_subset.isin(df_obtener_proyectos_existentes_columnas_requeridas_subset.to_dict('list')).all(1)]
          
            actualizacion_proyectos = resultado_final.to_dict(orient='records')
            actualizacion_proyectos_log = resultado_final[['nombre','contrato']].to_dict(orient='records')
            return {'respuesta':resultado_final,'log':actualizacion_proyectos_log,'estado':2} if len(actualizacion_proyectos) > 0 else {'respuesta':actualizacion_proyectos,'estado':0}
        
        return {'respuesta': [], 'estado': 0}  
    
    def obtener_proyectos_registro(self):
        
            if len(self.proyectos_existentes) == 0:
                df_proyectos = pd.DataFrame(self.__proyectos)
                proyectos_condicion = df_proyectos.apply(lambda col: col != 0)
                resultado = df_proyectos[proyectos_condicion.all(axis=1)]
                proyectos_a_insertar = resultado[['nombre','contrato']].to_dict(orient='records')
                proyectos_registro =  resultado.to_dict(orient='records')
                return {'respuesta':proyectos_registro,'log':proyectos_a_insertar,'estado':1} if len(proyectos_registro)>0 else {'respuesta':proyectos_registro,'log':[],'estado':0}
        
            validacion_contenido = self.comparacion_existe_datos(self.proyectos_existentes)
            
            if validacion_contenido:
                
                df_proyectos = pd.DataFrame(self.__proyectos)
                df_obtener_proyectos_existentes = pd.DataFrame(self.proyectos_existentes)

                columnas_comparar = ['nombre', 'responsable_id','fecha_inicio', 'fecha_final', 'valor_inicial', 'valor_final', 'duracion', 'contrato', 'estado_id', 'objeto', 'unidad_gerencia_id', 'unidad_organizativa_id', 'cliente_id', 'ceco_id']   
                                                       
                filtro_personalizado_subset,df_obtener_proyectos_existentes_columnas_requeridas_subset = self.comparacion_columnas_filtro(df_proyectos,df_obtener_proyectos_existentes,columnas_comparar)
                
                obtener_ceco_registro = filtro_personalizado_subset[
                    ~filtro_personalizado_subset.apply(lambda x: (
                        (x['ceco_id'] in set(df_obtener_proyectos_existentes_columnas_requeridas_subset['ceco_id'])) 
                    ), axis=1)
                ]
                
                obtner_respuesta_registro = obtener_ceco_registro.to_dict(orient='records')
                log_obtner_respuesta_registro = obtener_ceco_registro[['nombre','contrato']].to_dict(orient='records')
                
                return {'respuesta':obtner_respuesta_registro,'log':log_obtner_respuesta_registro,'estado':1} if len(obtner_respuesta_registro) > 0 else {'respuesta':obtner_respuesta_registro,'log':log_obtner_respuesta_registro,'estado':0}
        
            return {'respuesta': [], 'estado': 0}
 
    def obtener_no_sufrieron_cambios(self):
            validacion_contenido = self.comparacion_existe_datos(self.proyectos_existentes_por_estado)

            if validacion_contenido:
                df_proyectos = pd.DataFrame(self.__proyectos)
                df_obtener_proyectos_existentes = pd.DataFrame(self.proyectos_existentes_por_estado)
                
                df_proyectos['fecha_inicio'] = df_proyectos['fecha_inicio'].astype(str)
                df_obtener_proyectos_existentes['fecha_inicio'] = df_obtener_proyectos_existentes['fecha_inicio'].astype(str)
                
                df_proyectos['fecha_final'] = df_proyectos['fecha_final'].astype(str)
                df_obtener_proyectos_existentes['fecha_final'] = df_obtener_proyectos_existentes['fecha_final'].astype(str)

                # Selecciona solo las columnas requeridas
                columnas_requeridas = ['nombre', 'responsable_id','fecha_inicio', 'fecha_final', 'valor_inicial', 'valor_final', 'duracion', 'contrato', 'estado_id', 'objeto', 'unidad_gerencia_id', 'unidad_organizativa_id', 'cliente_id', 'ceco_id']
                # Utiliza merge para encontrar las filas que son iguales en ambos DataFrames
                no_sufren_cambios = pd.merge(
                    df_proyectos[columnas_requeridas],
                    df_obtener_proyectos_existentes[columnas_requeridas],
                    how='inner',
                    on=columnas_requeridas
                )
                
                proyectos_sin_cambio = no_sufren_cambios[['nombre','contrato']].to_dict(orient='records')
                conteo_proyectos_sin_cambios = len(proyectos_sin_cambio)
                
               
                proyectos_sin_cambios = {
                    'respuesta': [{
                    'sin_cambios':proyectos_sin_cambio,
                    'mensaje': f"Se econtraron un total de {conteo_proyectos_sin_cambios} proyectos sin cambios" if conteo_proyectos_sin_cambios > 1 else f"Se encontro ({conteo_proyectos_sin_cambios}) un proyecto sin cambios"
                }],
                    'estado':3
                } if conteo_proyectos_sin_cambios > 0 else {'respuesta':proyectos_sin_cambio,'estado':0}
               
            else:
                proyectos_sin_cambios = {'respuesta': [], 'estado': 0}
            return proyectos_sin_cambios
    # metodos para obtener los valores de las validaciones
    def obtener_exepcion_responsable_no_existe(self):
        obtener_responsable_id_no_existe = self.atrapar_una_excepcion('responsable_id',GlobalMensaje.NIT_NO_ENCONTRADO.value)
        return obtener_responsable_id_no_existe
    
    # se obtiene los proyectos que se le han enviado y al compararlo no existen en la base de datos 
    # (proyecto_ceco, proyecto_estado,proyecto cliente y la relacion entre proyecto_unidad_gerencia y proyecto_unidad_organizativa)
    def proyectos_unidad_excepcion(self):
        obtener_excepcion_fechas = self.atrapar_excepciones('unidad_organizativa_id','unidad_gerencia_id',ProyectosMensaje.GERENCIAS_NO_VINCULADAS.value)
        return obtener_excepcion_fechas
    
    def cliente_no_existe(self):
        obtener_excepcion_cliente_proyectos = self.atrapar_una_excepcion('cliente_id', GlobalMensaje.no_existen('ID Cliente'))
        return obtener_excepcion_cliente_proyectos
    
    def estado_proyecto_no_existe(self):
        obtener_excepcion_estado = self.atrapar_una_excepcion('estado_id',GlobalMensaje.no_existen('ID Estado'))
        return obtener_excepcion_estado
    
    def id_proyectos_invalidos(self):
        obtener_excepcion_proyectos = self.atrapar_una_excepcion("ceco_id",f"{GlobalMensaje.no_existen('ID Proyecto')}, o no se encuentre actualmente registrado en los proyectos.")
        return obtener_excepcion_proyectos

    
    def atrapar_una_excepcion(self,llave:str,mensaje:str):
        
        df_proyectos = pd.DataFrame(self.__proyectos)
        respuesta_default = {'respuesta': [], 'estado': 0}
        
        if df_proyectos.empty:
            return  respuesta_default
        
        # Verificar si hay algún valor igual a 0 en la columna 'ceco_id'
        no_existen = (df_proyectos[llave] == 0).any()
        
        if not no_existen:
            return  respuesta_default
        
        obtener_excepcion = df_proyectos[df_proyectos[llave] == 0][['proyecto_id_erp']].to_dict(orient='records')
        
        if len(obtener_excepcion) == 0:
            return  respuesta_default
        
        return {'respuesta':  {'exepcion':obtener_excepcion, 'mensaje':mensaje},'estado':3}
    
    def atrapar_excepciones(self,llave_inicial,llave_final,mensaje):
        df_proyectos = pd.DataFrame(self.__proyectos)
        
        if df_proyectos.empty:
              return {'respuesta': [],'estado': 0}
          
        filtro_personalizado = df_proyectos[
                (df_proyectos[[llave_inicial, llave_final]] == 0).any(axis=1)
            ]
          
        obtener_monto = filtro_personalizado[['proyecto_id_erp']].to_dict(orient='records')

          
        if len(obtener_monto) == 0:
              return {'respuesta': [],'estado': 0}
          
        return {'respuesta':  {'exepcion':obtener_monto, 'mensaje':mensaje},'estado':3}
    
    def obtener_montos_invalidas(self):
        obtener_excepcion_fechas = self.atrapar_excepciones('valor_inicial','valor_final','Por favor, verifique que los montos de valor inicial y final sean superior a 0.')
        return obtener_excepcion_fechas
    
    
    def obtener_fechas_invalidas(self):
        obtener_fecha_invalida = self.atrapar_excepciones('fecha_inicio','fecha_final','Por favor, verifique que los campos de fecha tengan el formato indicado. (2024-01-20)')
        return obtener_fecha_invalida
    
    def obtener_usuario(self,proyecto):
        identificacion = proyecto['identificacion']
        usuario = self.encontrar_id_usuario(int(identificacion))['id_usuario']
        # Verificar si el valor es un número y no es NaN
        return usuario if usuario else 0

    def obtener_ids_unidad_gerencia_organizativa(self,proyecto):
        unidad_gerencia_id = proyecto['unidad_gerencia_id']
        unidad_organizativa_id = proyecto['unidad_organizativa_id']
        unidades_organizativas = self.ids_unidad_organizativas(unidad_gerencia_id, unidad_organizativa_id)
        
        return {
            'unidad_organizativa_id_erp': unidades_organizativas['unidad_organizativa_id_erp'] if unidades_organizativas else 0,
            'unidad_gerencia_id_erp': unidades_organizativas['unidad_gerencia_id_erp'] if unidades_organizativas else 0
            }

    def obtener_id_entidad(self, proyecto, entidad):
        id_entidad = proyecto[f'{entidad}_id']
        entidad_obj = getattr(self,f'obtener_estado_proyecto_{entidad}')(id_entidad)
        return entidad_obj.to_dict().get("id") if entidad_obj else 0

    def obtener_estructuracion_fechas(self,proyecto):
        fecha_inicio = proyecto['fecha_inicio']
        fecha_final = proyecto['fecha_final']
        return self.calcular_estructuracion_fechas(fecha_inicio, fecha_final)

    def obtener_valores_proyectos(self,proyecto):
        valor_inicial = proyecto['valor_inicial']
        valor_final = proyecto['valor_final']
        return self.validar_valores(valor_inicial,valor_final)

    def procesar_proyecto(self, proyecto):
        unidades_administrativas = self.obtener_ids_unidad_gerencia_organizativa(proyecto)
        obtener_fechas = self.obtener_estructuracion_fechas(proyecto)
        monto = self.obtener_valores_proyectos(proyecto)

        return {
            "proyecto_id_erp":proyecto['ceco_id'],
            "ceco_id":self.obtener_id_entidad(proyecto,'ceco'),
            "nombre": proyecto["nombre"],
            "objeto":proyecto["objeto"],
            "contrato":proyecto["contrato"],
            "responsable_id": self.obtener_usuario(proyecto),
            "unidad_organizativa_id": unidades_administrativas['unidad_organizativa_id_erp'],
            "unidad_gerencia_id": unidades_administrativas['unidad_gerencia_id_erp'],
            "cliente_id": self.obtener_id_entidad(proyecto,'cliente'),
            "estado_id": self.obtener_id_entidad(proyecto,'estado'),
            "fecha_inicio":obtener_fechas['fecha_inicio'] if obtener_fechas else 0,
            "fecha_final":obtener_fechas['fecha_fin'] if obtener_fechas else 0,
            "duracion":obtener_fechas['duracion'] if obtener_fechas else 0,
            "valor_inicial":monto['valor_inicial'] if monto else 0,
            "valor_final":monto['valor_final'] if monto else 0
        }
    
    # metodos para validaciones de otros campos
    def calcular_estructuracion_fechas(self,fecha_inicio, fecha_fin):
        try:
            # Verificar si las fechas ya son objetos datetime
            if isinstance(fecha_inicio, datetime) and isinstance(fecha_fin, datetime):
                # Verificar la condición de que fecha_inicio debe ser menor o igual a fecha_fin
                if fecha_inicio > fecha_fin:
                    return None

                # Calcular la duración en meses
                duracion_meses = (fecha_fin.year - fecha_inicio.year) * 12 + fecha_fin.month - fecha_inicio.month

                # Retornar el objeto con las fechas y la duración
                return {
                    'fecha_inicio': fecha_inicio.strftime('%Y-%m-%d'),
                    'fecha_fin': fecha_fin.strftime('%Y-%m-%d'),
                    'duracion': duracion_meses
                }
            else:
                # Convertir las fechas a objetos datetime
                fecha_inicio = datetime.strptime(str(fecha_inicio), '%Y-%m-%d')
                fecha_fin = datetime.strptime(str(fecha_fin), '%Y-%m-%d')

                # Verificar la condición de que fecha_inicio debe ser menor o igual a fecha_fin
                if fecha_inicio > fecha_fin:
                    return None

                # Calcular la duración en meses
                duracion_meses = (fecha_fin.year - fecha_inicio.year) * 12 + fecha_fin.month - fecha_inicio.month

                # Retornar el objeto con las fechas y la duración
                return {
                    'fecha_inicio': fecha_inicio.strftime('%Y-%m-%d'),
                    'fecha_fin': fecha_fin.strftime('%Y-%m-%d'),
                    'duracion': duracion_meses
                }

        except ValueError:
                return None
    
    def validar_valores(self, valor_inicial, valor_final):
        try:
            # Convertir los valores a números enteros
            valor_inicial = float(valor_inicial)
            valor_final = float(valor_final)

            # Validar que ambos valores sean no negativos
            if valor_inicial < 0 or valor_final < 0:
                return None

            # Validar que el valor inicial sea menor o igual al valor final
            if valor_inicial > valor_final:
                return None

            # Si todo está bien, retornar los valores como un objeto
            return {
                'valor_inicial': valor_inicial,
                'valor_final': valor_final
            }

        except ValueError as e:
            # Manejar el caso en que los valores no sean válidos
            return {
                'error': f"Error de validación: {str(e)}"
            }  
    
    # metodos para consultas o para las operaciones de la base de datos
    def insertar_inforacion(self, proyectos_nuevos: List):
        try:
            registros_proyectos = proyectos_nuevos['respuesta']
            num_proyectos = len(registros_proyectos)
            
            if num_proyectos > 0:
                proyectos_log = proyectos_nuevos['log']
                # insertar_informacion = insert(ModeloProyectos,registros_proyectos)
                # session.execute(insertar_informacion)
                # session.commit()
           
                return {    
                        'registro':proyectos_log,
                        'mensaje': f'Se han realizado la carga de {num_proyectos} registros exitosos.' if num_proyectos > 1 else  f'Se ha cargado un ({num_proyectos}) registro exitoso.'
                        }

            return "No se han registrado datos"
        except SQLAlchemyError as e:
              print(f"Se produjo un error de SQLAlchemy: {str(e)}")
              return {'problema':'Lo sentimos hemos tenido un problema, estamos trabajando en ello.',
                        'err':str(e)}
        

    def actualizar_informacion(self, actualizar_proyectos):
        try:
            
            actualizacion_proyectos_respuesta = actualizar_proyectos['respuesta']
            num_proyectos = len(actualizacion_proyectos_respuesta)
            
            if num_proyectos > 0  :
                actualizacion_proyectos = actualizar_proyectos['log']
                # session.bulk_update_mappings(ModeloProyectos, actualizacion_proyectos_respuesta)
                # session.commit()
                return {    
                        'actualizacion':actualizacion_proyectos,
                        'mensaje':f'Se han realizado un total de {num_proyectos} actualizaciones exitosas.' if num_proyectos > 1 else f'Se ha realizado {num_proyectos} actualización exitosa.'
                        }
            return "No se han actualizado datos"
        except SQLAlchemyError as e:
              print(f"Se produjo un error de SQLAlchemy: {str(e)}")
              return {'problema':'Lo sentimos hemos tenido un problema, estamos trabajando en ello.',
                        'err':str(e)
                        }
    
    def encontrar_id_usuario(self, identificacion):
        gestor_excel = GestorExcel()
        id_gerente_encargado = gestor_excel.obtener_id_usuario(identificacion) 
        return id_gerente_encargado
    
    def ids_unidad_organizativas(self, id_unidad_gerencia, id_unidad_organizativa):
        unidad_organizativa_alias = aliased(ProyectoUnidadOrganizativa)
        unidad_gerencia_alias = aliased(ProyectoUnidadGerencia)

        result = (
            session.query(
                unidad_organizativa_alias.id.label('unidad_organizativa_id_erp'),
                unidad_gerencia_alias.id.label('unidad_gerencia_id_erp')
            )
            .join(unidad_gerencia_alias, unidad_gerencia_alias.id == unidad_organizativa_alias.gerencia_id)
            .filter(
                unidad_organizativa_alias.unidad_organizativa_id_erp == id_unidad_organizativa,
                unidad_gerencia_alias.unidad_gerencia_id_erp == id_unidad_gerencia
            )
            .first()
        )

        if result:
            return result._asdict()
        else:
            return None
        
    def obtener_estado_proyecto_cliente(self, id_erp_cliente):
             return (
                session.query(ProyectoCliente).filter(
                and_(
                       ProyectoCliente.cliente_id_erp == id_erp_cliente,
                       ProyectoCliente.estado == 1
                )
                ).first()
           )
        
    def obtener_estado_proyecto_estado(self,id_estado):
            return (
                session.query(ProyectoEstado).filter(
                and_(
                       ProyectoEstado.estado_id_erp == id_estado,
                       ProyectoEstado.estado == 1
                )
                ).first()
           )
            
    def obtener_estado_proyecto_ceco(self,id_ceco):
            return (
                session.query(ProyectoCeco).filter(
                and_(
                       ProyectoCeco.ceco_id_erp == id_ceco,
                       ProyectoCeco.estado == 1
                )
                ).first()
           )
        