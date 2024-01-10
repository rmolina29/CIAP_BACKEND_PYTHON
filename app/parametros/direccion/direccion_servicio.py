from typing import List
from app.parametros.direccion.esquema.proyecto_unidad_organizativa import Archivo
from app.parametros.direccion.model.proyecto_unidad_organizativa import ProyectoUnidadOrganizativa
from app.database.db import session
from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError


class Direccion:
    def __init__(self,data:List[Archivo]) -> None:
        self.__direcion_excel = data
        self.__obtener_unidad_organizativa_existentes = self.obtener_direccion()
        self.__unidad_organizativa = self.proceso_informacion_con_id_gerencia()
    
    def transacciones(self):
        try:
            novedades_de_unidad_organizativa = self.comparacion_unidad_organizativa()
            # informacion a insertar
            lista_insert = novedades_de_unidad_organizativa.get("insercion_datos")
            unidad_organizativa_update = novedades_de_unidad_organizativa.get("actualizacion_datos")
            sin_cambios = novedades_de_unidad_organizativa.get("excepciones_campos_unicos")
            excepciones_id_usuario = novedades_de_unidad_organizativa.get("exepcion_unidad_organizativa")

            log_transaccion_registro = self.insertar_informacion(lista_insert)
            log_transaccion_actualizar = self.actualizar_informacion(unidad_organizativa_update)

            
            log_transaccion_registro_unidad_organizativa = {
                "log_transaccion_excel": {
                    "unidad_organizativa_registradas": log_transaccion_registro,
                    "unidad_organizativas_actualizadas": log_transaccion_actualizar,
                    "unidad_organizativas_sin_cambios": sin_cambios,
                    "unidad_organizativa_nit_no_existe": excepciones_id_usuario,
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
                unidad_organizativa_actualizacion,
            ) = self.filtrar_unidad_organizativa()

            resultado = {
                "insercion_datos": registro_unidad_organizativa,
                "actualizacion_datos": unidad_organizativa_actualizacion,
                "excepciones_campos_unicos": excepciones_unidad_organizativa,
                "exepcion_unidad_organizativa": unidad_organizativa_id_erp,
            }

            return resultado
        except Exception as e:
            raise (f"Error al realizar la comparación: {str(e)}")
      
    def filtrar_unidad_organizativa(self):
        excepciones_unidad_organizativa = self.obtener_no_sufrieron_cambios()
        unidad_organizativa_id_erp = self.unidad_organizativa_id_erp()
        registro_unidad_organizativa = self.filtrar_unidad_organizativas_nuevas(excepciones_unidad_organizativa)
        unidad_organizativa_actualizacion = self.obtener_unidad_organizativa_actualizacion(
            excepciones_unidad_organizativa
        )

        return (
            excepciones_unidad_organizativa,
            unidad_organizativa_id_erp,
            registro_unidad_organizativa,
            unidad_organizativa_actualizacion,
        )
    
    def unidad_organizativa_id_erp(self):
        id_gerencia = [
            {
                "unidad_organizativa_id_erp": item["unidad_organizativa_id_erp"],
                "nombre": item["nombre"],
            }
            for item in self.__unidad_organizativa
            if ((item["gerencia_id"]) is None)
        ]
        return id_gerencia
    
    def obtener_no_sufrieron_cambios(self):
        no_sufieron_cambios = [
            {
                "unidad_organizativa_id_erp": item["unidad_organizativa_id_erp"],
                "nombre": item["nombre"],
            }
            for item in self.__unidad_organizativa
            for d in self.__obtener_unidad_organizativa_existentes
            if ((item["nombre"].upper()) == (d["nombre"].upper())) 
        ]
        
        return no_sufieron_cambios
    
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
            gerencias_nuevas = [
                item
                for item in self.__unidad_organizativa
                if (
                    item["gerencia_id"] is not None
                    and (item["unidad_organizativa_id_erp"] or item["nombre"])
                    not in {
                        (g["unidad_organizativa_id_erp"] or g["nombre"])
                        for g in self.__obtener_unidad_organizativa_existentes
                    }
                )
            ]
    
            # filtro de excepciones atrapada de datos unicos, el cual obtiene la informacion nueva y la filtra con las exepciones que existen
            filtro_unidad_organizativa = self.direccion_mapeo_excepciones(
            gerencias_nuevas, excepciones_unidad_organizativa
            )
            
            return filtro_unidad_organizativa
        except Exception as e:
            print(f"Se produjo un error: {str(e)}")
        
    def obtener_unidad_organizativa_actualizacion(self, excepciones_unidad_organizativa):
        gerencias_actualizacion = [
            {
                "id": g["id"],
                "unidad_organizativa_id_erp": item["unidad_organizativa_id_erp"],
                "nombre": item["nombre"],
                "gerencia_id": item["gerencia_id"],
            }
            for item in self.__unidad_organizativa
            for g in self.__obtener_unidad_organizativa_existentes
            if (
                item["gerencia_id"] is not None
                and item["unidad_organizativa_id_erp"].strip().lower()
                == g["unidad_organizativa_id_erp"].strip().lower()
            )
        ]

        filtrado_actualizacion = self.direccion_mapeo_excepciones(
            gerencias_actualizacion, excepciones_unidad_organizativa
        )
        
        
        return filtrado_actualizacion
    
    def direccion_mapeo_excepciones(self, direccion, excepciones):
        # filtro de excepciones atrapada de datos unicos, el cual obtiene la informacion nueva y la filtra con las exepciones que existen
        filtro_direccion = [
            item
            for item in direccion
            if {
                "unidad_organizativa_id_erp": item["unidad_organizativa_id_erp"],
                "nombre": item["nombre"],
            }
            not in excepciones
        ]
    
        return filtro_direccion
    
    def insertar_informacion(self, novedades_unidad_organizativa: List):
        if len(novedades_unidad_organizativa) > 0:
            session.bulk_insert_mappings(ProyectoUnidadOrganizativa, novedades_unidad_organizativa)
            return novedades_unidad_organizativa

        return "No se han registrado datos"

    def actualizar_informacion(self, actualizacion_gerencia_unidad_organizativa):
        if len(actualizacion_gerencia_unidad_organizativa) > 0:
            session.bulk_update_mappings(ProyectoUnidadOrganizativa, actualizacion_gerencia_unidad_organizativa)
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
                            "gerencia_id": None,
                        }
                    )
            return resultados
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e