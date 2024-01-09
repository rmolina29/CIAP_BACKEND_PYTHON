from sqlalchemy import Column, Integer, Text

from app.database.db import Base


class ProyectoUnidadOrganizativa(Base):
    __tablename__ = "proyecto_unidad_organizativa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    unidad_organizativa_id_erp = Column(Text, nullable=False, unique=True)
    nombre = Column(Text, unique=True, nullable=True)
    gerencia_id = Column(Integer, nullable=False)

    def __init__(self, id, unidad_organizativa_id_erp, nombre, gerencia_id, estado):
        self.id = id
        self.unidad_organizativa_id_erp = unidad_organizativa_id_erp
        self.nombre = nombre
        self.gerencia_id = gerencia_id

    def to_dict(self):
        return {
            "id": self.id,
            "unidad_organizativa_id_erp": self.unidad_organizativa_id_erp,
            "nombre": self.nombre,
            "gerencia_id": self.gerencia_id,
        }
