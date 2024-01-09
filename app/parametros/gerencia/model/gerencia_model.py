from sqlalchemy import Column, Integer, Text, TIMESTAMP, func
from sqlalchemy.dialects.mysql import TINYINT

# from sqlalchemy.ext.declarative import declarative_base
from app.database.db import Base

# Base = declarative_base()


class ProyectoUnidadGerencia(Base):
    
    __tablename__ = "proyecto_unidad_gerencia"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    unidad_gerencia_id_erp = Column(Text, nullable=False, unique=True)
    nombre = Column(Text, unique=True, nullable=True)
    responsable_id = Column(Integer, nullable=False)
    estado = Column(
        TINYINT(unsigned=True), default=1
    ) 

    def __init__(self, id, unidad_gerencia_id_erp,nombre,responsable_id):
        self.id = id
        self.unidad_gerencia_id_erp = unidad_gerencia_id_erp
        self.nombre = nombre
        self.responsable_id = responsable_id
        

    
    def to_dict(self):
        return {
            "id":self.id,
            "unidad_gerencia_id_erp": self.unidad_gerencia_id_erp,
            "nombre": self.nombre,
            "responsable_id":self.responsable_id
        }

