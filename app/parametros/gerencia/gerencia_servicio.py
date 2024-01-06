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

            novedades_de_gerencia = self.comparacion_gerencia(
                data_gerencia, obtener_gerencia_existente
            )

            # informacion a insertar
            lista_insert = novedades_de_gerencia.get("insert")
            gerencia_update = novedades_de_gerencia.get("update")
            
            # session.bulk_insert_mappings(ProyectoUnidadGerencia, novedades_de_gerencia)
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
                # actualizar informacion si hay diferencdias en los archivos, realizamos un
                return "listas iguales"
            else:
                # si son diferentes las listas, es decir hay nuevas insercion las obtiene y las insertas (solamente las que seran diferentes)
                gerencias_nuevas = [item for item in data if item not in gerencia]

                gerencias_actualizacion = [item for item in data if item in gerencia]
                return {"insert": gerencias_nuevas, "update": gerencias_actualizacion}

        except Exception as e:
            raise (f"Error al realizar la comparacion: {str(e)}")
