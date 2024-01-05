from app.parametros.gerencia.model.gerencia_model import ProyectoUnidadGerencia
from app.database.db import session


class Gerencia:
    def __init__(self) -> None:
        pass

    def obtener(self):
        informacion_gerencia = session.query(ProyectoUnidadGerencia).all()
        # Convertir lista de objetos a lista de diccionarios
        gerencia_data = [gerencia.to_dict() for gerencia in informacion_gerencia]
        return gerencia_data
