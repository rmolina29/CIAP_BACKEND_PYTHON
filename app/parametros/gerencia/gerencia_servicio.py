from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from app.database.db import session
from typing import List
from app.parametros.gerencia.esquema.archivo_esquema import Archivo


class Gerencia:
    def __init__(self) -> None:
        pass

    def obtener(self):
        informacion_gerencia = session.query(ProyectoUnidadGerencia).all()
        # Convertir lista de objetos a lista de diccionarios
        gerencia_data = [gerencia.to_dict() for gerencia in informacion_gerencia]
        return gerencia_data

    def registrar_informacion(self, data: List[Archivo]):
        try:
            data_gerencia = [archivo.model_dump() for archivo in data]

            obtener_gerencia_existente = self.obtener()

            info = self.comparacion_gerencia(data_gerencia, obtener_gerencia_existente)

            print(info)
            # session.bulk_insert_mappings(ProyectoUnidadGerencia, data_gerencia)
            # session.commit()
            return "Multi-insert realizado con éxito."
        except Exception as e:
            session.rollback()
            raise (f"Error al realizar el multi-insert: {str(e)}")
        finally:
            session.close()

    def comparacion_gerencia(self, data, gerencia):
        try:
            if len(data) == len(gerencia):
                return "listas iguales"
            else:
                diferentes_en_lista1 = [item for item in data if item not in gerencia]
                return {
                    "Elementos en lista1 que no están en lista2:": diferentes_en_lista1
                }

        except Exception as e:
            raise (f"Error al realizar la comparacion: {str(e)}")
