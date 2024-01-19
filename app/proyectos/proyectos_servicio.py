import math
from app.proyectos.model.proyectos import ModeloProyectos
from app.parametros.ceco.model.ceco_model import ProyectoCeco
from app.parametros.estado.model.estado_model import ProyectoEstado
from app.parametros.cliente.model.cliente_model import ProyectoCliente
from app.parametros.direccion.model.proyecto_unidad_organizativa import ProyectoUnidadOrganizativa
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from app.parametros.gerencia.model.datos_personales_model import UsuarioDatosPersonales
from app.database.db import session
from typing import List
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
from fastapi import  UploadFile
import pandas as pd
from sqlalchemy.orm import aliased
from datetime import datetime, timedelta

class Proyectos:
    
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        # con esta data se hara la comparacion para los datos que estan en registros
        self.proyectos_existentes = self.obtener()
        # y esta data se realizara la comparacion contra los que tiene para actualizar
        self.proyectos_existentes_por_estado = self.obtener_por_estado_proyectos()
        resultado_estructuracion = self.__proceso_de_proyectos_estructuracion()
        self.__proyectos_excel_duplicada = resultado_estructuracion['duplicados']
        self.__proyectos_excel = resultado_estructuracion['resultado']
        # data procesada contiene los datos en 0 de las id que no existen en otras tablas
        self.__proyectos = self.inforamcion_proyectos_procesada()
        
        
    
    def comparacion_existe_datos(self,obtener_proyectos_existentes)->bool:
        return len(self.__proyectos) > 0 and len(obtener_proyectos_existentes) > 0
    
      
    def transacciones(self):
 
        log_transaccion_registro_proyecto = {
                    "log_transaccion_excel": {
                        'Respuesta':[
                            {
                                "proyecto_registradas": self.obtener_proyectos_registro(),
                                "proyectos_actualizadas": self.obtener_proyectos_actualizacion(),
                                "proyectos_sin_cambios": [],
                            }
                        ],
                        'errores':{

                            "proyecto_responsable_no_existe": [],
                            "proyecto_cliente_no_existe":[],
                            "proyectos_unidad_administrativas":[],
                            "proyecto_filtro_nit_invalido":self.validacion_informacion_identificacion()['nit_invalido'],
                            # "proyectos_existentes":excepciones_proyecto,
                            "proyectos_duplicadas": self.__proyectos_excel_duplicada
                        }
                    },
                    'estado':{

                        'id':0
                    }
                }
        
        return log_transaccion_registro_proyecto
    
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
        
        if df_excel.empty:
                return {'resultado': [], 'duplicados': []}

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
        
        df_filtered = df_excel.dropna()


        duplicados_id_proyecto_erp = df_filtered.duplicated(subset='ceco_id', keep=False)
        # Filtrar DataFrame original
        resultado = df_filtered[~(duplicados_id_proyecto_erp)].to_dict(orient='records')
        
        duplicados = df_filtered[(duplicados_id_proyecto_erp)].to_dict(orient='records')
        
        duplicated = pd.DataFrame(duplicados)
        
        if duplicated.isnull().any(axis=1).any():
            lista_gerencias = []
        else:
            lista_gerencias = [
                {
                    'ceco_id': item['ceco_id'],
                }
                for item in duplicados
            ]
        
        return {'resultado':resultado,'duplicados':lista_gerencias}
    
    
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
                    
            return {'nit_invalido': proyecto_identificacion_incorrecta, 'proyecto_filtro_datos': proyecto_filtro_datos,'estado':0}

        except Exception as e:
            raise Exception(f"Error al realizar la comparación: {str(e)}") from e
    
    # def gerencia_usuario_procesada(self):
    #     try:
    #         proyectos_excel = self.validacion_informacion_identificacion()
    #         resultados = []
    #         for proyecto in proyectos_excel['proyecto_filtro_datos']:
                
    #             identificacion = proyecto['identificacion']
    #             unidad_gerencia_id = proyecto['unidad_gerencia_id']
    #             unidad_organizativa_id = proyecto['unidad_organizativa_id']
    #             estado_id = proyecto['estado_id']
    #             unidad_cliente_id = proyecto['cliente_id']
    #             fecha_inicio = proyecto['fecha_inicio']
    #             fecha_final = proyecto['fecha_final']
    #             # Verificar si el valor es un número y no es NaN
    #             usuario = self.encontrar_id_usuario(int(identificacion))
    #             ids_unidad_gerencia_y_organizativa = self.ids_unidad_organizativas(unidad_gerencia_id,unidad_organizativa_id)
    #             unidad_estado_id = self.obtener_estado_proyecto_estado(estado_id)
    #             id_cliente = self.obtener_estado_proyecto_cliente(unidad_cliente_id)
    #             calcular_estructuracion_fechas = self.calcular_estructuracion_fechas(fecha_inicio,fecha_final)
           
                
    #             resultados.append({
    #                 "nombre": proyecto["nombre"],
    #                 "responsable_id": usuario.to_dict().get("id_usuario") if usuario else 0,
    #                 "unidad_organizativa_id_erp":ids_unidad_gerencia_y_organizativa["unidad_organizativa_id_erp"] if ids_unidad_gerencia_y_organizativa else 0,
    #                 "unidad_gerencia_id_erp":ids_unidad_gerencia_y_organizativa['unidad_gerencia_id_erp'] if ids_unidad_gerencia_y_organizativa else 0,
    #                 "cliente_id":id_cliente.to_dict().get("id") if id_cliente else 0,
    #                 "estado_id":unidad_estado_id.to_dict().get("id")  if unidad_estado_id else 0,
                    
    #                 })
            
    #         return resultados

    #     except Exception as e:
    #         print(e)
    #         session.rollback()
    #         raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e
    
    def inforamcion_proyectos_procesada(self):
        try:
            proyectos_excel = self.validacion_informacion_identificacion()
            resultados = []

            for proyecto in proyectos_excel['proyecto_filtro_datos']:
                resultados.append(self.procesar_proyecto(proyecto))

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
        
    # metodos para obtener las funcionalidades
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
            
            resultado_filtro_actualizacion = filtro_actualizacion[['id', 'ceco_id', 'nombre_x', 'objeto_x', 'contrato_x', 'responsable_id_x',
                                                       'unidad_organizativa_id_x', 'unidad_gerencia_id_x', 'cliente_id_x',
                                                       'estado_id_x', 'fecha_inicio_x', 'fecha_final_x', 'duracion_x',
                                                       'valor_inicial_x','valor_final_x']].rename(
                columns=lambda x: x.replace('_x', '') if x.endswith('_x') else x
            )
                                                       
            columnas_comparar = ['id', 'nombre', 'responsable_id','fecha_inicio', 'fecha_final', 'valor_inicial', 'valor_final', 'duracion', 'contrato', 'estado_id', 'objeto', 'unidad_gerencia_id', 'unidad_organizativa_id', 'cliente_id', 'ceco_id']                                          
            
            filtro_personalizado_subset,df_obtener_proyectos_existentes_columnas_requeridas_subset = self.comparacion_columnas_filtro(resultado_filtro_actualizacion,df_obtener_proyectos_existentes,columnas_comparar)
            
            resultado_final = filtro_personalizado_subset[~filtro_personalizado_subset.isin(df_obtener_proyectos_existentes_columnas_requeridas_subset.to_dict('list')).all(1)]
            # resultado_final = filtro_personalizado[~filtro_personalizado.isin(df_obtener_proyectos_existentes_v2.to_dict('list')).all(1)]
            return resultado_final.to_dict(orient='records')
        
        return []  
    
    def obtener_proyectos_registro(self):
        
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

                return obtener_ceco_registro.to_dict(orient='records')
        
            return []
 
    # metodos para obtener los valores de las validaciones
    
    def obtener_usuario(self,proyecto):
        identificacion = proyecto['identificacion']
        usuario = self.encontrar_id_usuario(int(identificacion))
        # Verificar si el valor es un número y no es NaN
        return usuario.to_dict().get("id_usuario") if usuario else 0

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

    def procesar_proyecto(self,proyecto):
        
        unidades_administrativas = self.obtener_ids_unidad_gerencia_organizativa(proyecto)
        obtener_fechas = self.obtener_estructuracion_fechas(proyecto)
        monto = self.obtener_valores_proyectos(proyecto)
        
        return {
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
    # metodos para consultas a la base de datos
    def encontrar_id_usuario(self, identificacion):
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
        
        #      return (
        #     session.query(UsuarioDatosPersonales)
        #     .filter(
        #         and_(
        #             UsuarioDatosPersonales.identificacion == identificacion,
        #             UsuarioDatosPersonales.estado == 1,
        #         )
        #     )
        #     .first()
        # )