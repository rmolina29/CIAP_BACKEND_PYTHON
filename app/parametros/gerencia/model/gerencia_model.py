from sqlalchemy import Column, Integer, Text, TIMESTAMP, func
from sqlalchemy.dialects.mysql import TINYINT

# from sqlalchemy.ext.declarative import declarative_base
from app.database.db import Base

# Base = declarative_base()


class ProyectoUnidadGerencia(Base):
    
    __tablename__ = "proyecto_unidad_gerencia"

    id = Column(Integer, primary_key=True, autoincrement=True)
    unidad_gerencia_id_erp = Column(Text, nullable=False, unique=True)
    unidad_gerencia_sig = Column(Integer, nullable=True)
    nombre = Column(Text, unique=True, nullable=True)
    responsable_id = Column(Integer, nullable=False)
    estado = Column(
        TINYINT(unsigned=True), default=1
    )  # Utiliza TINYINT(unsigned=True) para un TINYINT(1)
    fechasistema = Column(TIMESTAMP, server_default=func.current_timestamp())
    
    
    def __init__(self, id, unidad_gerencia_id_erp, unidad_gerencia_sig,nombre,responsable_id,estado,fechasistema):
        self.id = id
        self.unidad_gerencia_id_erp = unidad_gerencia_id_erp
        self.unidad_gerencia_sig = unidad_gerencia_sig
        self.nombre = nombre
        self.responsable_id = responsable_id
        self.estado = estado
        self.fechasistema = fechasistema
    

    
    def to_dict(self):
        return {
            "unidad_gerencia_id_erp": self.unidad_gerencia_id_erp,
            "nombre": self.nombre,
            "responsable_id":self.responsable_id
        }

