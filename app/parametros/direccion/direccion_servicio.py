from typing import List
from app.parametros.direccion.esquema.proyecto_unidad_organizativa import Archivo
from app.parametros.direccion.model.proyecto_unidad_organizativa import ProyectoUnidadOrganizativa
from app.database.db import session


class Direccion:
    def __init__(self,data:List[Archivo]) -> None:
        self.__direcion_excel = data
        self.__obtener_unidad_organizativa = self.obtener_direccion()
    
    def transacciones(self):
        return self.__obtener_unidad_organizativa
    
    
    def obtener_direccion(self):
        informacion_direccion = session.query(ProyectoUnidadOrganizativa).all()
        # Convertir lista de objetos a lista de diccionarios
        gerencia_data = [gerencia.to_dict() for gerencia in informacion_direccion]
        return gerencia_data
    
    
    
    def query_usuario(self):
        pass
    
    def proceso_informacion_con_id_gerencia(self):
        try:
            resultados = []
            for gerencia in self.__direcion_excel:
                usuario = self.query_usuario(gerencia["NIT"])
                if usuario:
                    resultados.append(
                        {
                            "unidad_gerencia_id_erp": gerencia[
                                "unidad_gerencia_id_erp"
                            ],
                            "nombre": gerencia["nombre"],
                            "responsable_id": usuario.to_dict().get("id_usuario"),
                        }
                    )
                else:
                    resultados.append(
                        {
                            "unidad_gerencia_id_erp": gerencia[
                                "unidad_gerencia_id_erp"
                            ],
                            "nombre": gerencia["nombre"],
                            "responsable_id": None,
                        }
                    )
            return resultados
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error al realizar la operación: {str(e)}") from e