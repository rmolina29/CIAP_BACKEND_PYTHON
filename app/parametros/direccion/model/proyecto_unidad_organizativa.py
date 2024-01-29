from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship
from app.database.db import Base
from sqlalchemy import SmallInteger
from sqlalchemy.dialects.postgresql import INTEGER

class ProyectoUnidadOrganizativa(Base):
    __tablename__ = "proyecto_unidad_organizativa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    unidad_organizativa_id_erp = Column(Text, nullable=False, unique=True)
    nombre = Column(Text, unique=True, nullable=True)
    gerencia_id = Column(Integer, ForeignKey('proyecto_unidad_gerencia.id'), nullable=False)
    estado = Column(Integer, default=1) 
    
    gerencia = relationship("ProyectoUnidadGerencia", back_populates="unidades_organizativas")

    def __init__(self, unidad_organizativa_id_erp, nombre, gerencia_id, estado):
        self.unidad_organizativa_id_erp = unidad_organizativa_id_erp
        self.nombre = nombre
        self.gerencia_id = gerencia_id
        self.estado = estado

    def to_dict(self):
        return {
            "id": self.id,
            "unidad_organizativa_id_erp": self.unidad_organizativa_id_erp,
            "nombre": self.nombre,
            "gerencia_id": self.gerencia_id,
            "estado": self.estado
        }
