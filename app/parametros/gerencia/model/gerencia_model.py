from sqlalchemy import Column, Integer, Text
from sqlalchemy.orm import relationship
from app.database.db import Base
class ProyectoUnidadGerencia(Base):
    
    __tablename__ = "proyecto_unidad_gerencia"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    unidad_gerencia_id_erp = Column(Text, nullable=False, unique=True)
    nombre = Column(Text, unique=True, nullable=True)
    responsable_id = Column(Integer, nullable=False)
    estado = Column(Integer, default=1)
    
    unidades_organizativas = relationship("ProyectoUnidadOrganizativa", back_populates="gerencia")
    
    def __init__(self, unidad_gerencia_id_erp, nombre, responsable_id, estado):
        self.unidad_gerencia_id_erp = unidad_gerencia_id_erp
        self.nombre = nombre
        self.responsable_id = responsable_id
        self.estado = estado
        
    def to_dict(self):
        return {
            "id": self.id,
            "unidad_gerencia_id_erp": self.unidad_gerencia_id_erp,
            "nombre": self.nombre,
            "responsable_id": self.responsable_id
        }
