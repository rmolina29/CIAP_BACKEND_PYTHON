from sqlalchemy import Column, Integer, Text
from app.database.db import Base
from sqlalchemy.dialects.mysql import TINYINT

class ProyectoEstado(Base):
    
    __tablename__ = "proyecto_estado"

    id = Column(Integer, primary_key=True, autoincrement=True)
    estado_id_erp = Column(Text, nullable=False, unique=True)
    descripcion = Column(Text, unique=False, nullable=False)
    estado = Column(
        TINYINT(unsigned=True), default=1
    ) 
   
    def __init__(self, id, estado_id_erp, descripcion,estado):
        self.id = id
        self.estado_id_erp = estado_id_erp
        self.descripcion = descripcion
        self.estado = estado

    def to_dict(self):
        return {
            "id": self.id,
            "estado_id_erp": self.estado_id_erp,
            "descripcion": self.descripcion,
        }
