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


class Proyectos:
    
    def __init__(self,file:UploadFile) -> None:
        self.__file = file
        self.proyectos_existentes = self.obtener()
        resultado_estructuracion = self.__proceso_de_proyectos_estructuracion()
        self.__proyectos_excel_duplicada = resultado_estructuracion['duplicados']
        self.__proyectos_excel = resultado_estructuracion['resultado']
        
    def transacciones(self):
        return self.proyectos_existentes
    
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
    
    
    def obtener_por_estado_gerencia(self, estado=1):
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
        
        
        df_excel["fecha_inicio"] = df_excel["fecha_inicio"].dt.strftime('%Y-%m-%d')
        df_excel["fecha_final"] = df_excel["fecha_final"].dt.strftime('%Y-%m-%d')
        
        df_filtered = df_excel.dropna()


        duplicados_id_proyecto_erp = df_filtered.duplicated(subset='ceco_id', keep=False)
        # Filtrar DataFrame original
        resultado = df_filtered[~(duplicados_id_proyecto_erp)].to_dict(orient='records')
        
        duplicados = df_filtered[(duplicados_id_proyecto_erp)].to_dict(orient='records')
        
        duplicated = pd.DataFrame(duplicados)
        
        if duplicated.isnull().any(axis=1).any():
            lista_gerencias = []
        else:
            lista_gerencias = [{**item, 'identificacion': int(item['identificacion'])} if isinstance(item.get('identificacion'), (int, float)) and not math.isnan(item.get('identificacion')) else item for item in duplicados]
        
        return {'resultado':resultado,'duplicados':lista_gerencias}
    
    
    def validacion_informacion_identificacion(self):
        try:
            if not self.__proyectos_excel:
                return {'nit_invalido': [], 'proyecto_filtrado_excel': [],'estado':0}

            proyecto_identificacion_incorrecta, proyecto_filtro_datos = [], []
            
            for item in self.__proyectos_excel:
                if isinstance(item.get('identificacion'), (int, float)):
                    proyecto_filtro_datos.append(item)
                else:
                    proyecto_identificacion_incorrecta.append(item)
            return {'nit_invalido': proyecto_identificacion_incorrecta, 'proyecto_filtro_datos': proyecto_filtro_datos,'estado':0}

        except Exception as e:
            raise Exception(f"Error al realizar la comparación: {str(e)}") from e
    
    def gerencia_usuario_procesada(self):
        try:
            proyectos_excel = self.validacion_informacion_identificacion()
            resultados = []
            for proyecto in proyectos_excel['proyecto_filtrado_excel']:
                identificacion = proyecto['identificacion']
                # Verificar si el valor es un número y no es NaN
                if isinstance(identificacion, (int, float)) and not math.isnan(identificacion):
                    usuario = self.encontrar_id_usuario(int(identificacion))

                    resultados.append({
                        "unidad_gerencia_id_erp": proyecto["unidad_gerencia_id_erp"],
                        "nombre": proyecto["nombre"],
                        "responsable_id": usuario.to_dict().get("id_usuario") if usuario else 0,
                    })
                    
            return resultados

        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e
        
        
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
    
    def ids_unidad_organizativas(self,id_unidad_gerencia,id_unidad_organizativa):
        return (
            session.query(
                ProyectoUnidadOrganizativa.id.label('id_unidad_organizativa'),
                ProyectoUnidadGerencia.id.label('id_unidad_de_gerencia')
            )
            .join(ProyectoUnidadGerencia, ProyectoUnidadGerencia.id == ProyectoUnidadOrganizativa.gerencia_id)
            .filter(
                ProyectoUnidadOrganizativa.unidad_organizativa_id_erp == id_unidad_organizativa,
                ProyectoUnidadGerencia.unidad_gerencia_id_erp == id_unidad_gerencia
            )
            .first()
        )