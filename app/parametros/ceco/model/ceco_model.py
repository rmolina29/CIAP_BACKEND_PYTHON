from sqlalchemy import Column, Integer, Text
from app.database.db import Base


class ProyectoCeco(Base):
    
    __tablename__ = "proyecto_ceco"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ceco_id_erp = Column(Text, nullable=False, unique=True)
    nombre = Column(Text, unique=True, nullable=True)
    descripcion = Column(Text,nullable = True)
   

    def __init__(self, id, ceco_id_erp, nombre,descripcion):
        self.id = id
        self.ceco_id_erp = ceco_id_erp
        self.nombre = nombre
        self.descripcion = descripcion


    def to_dict(self):
        return {
            "id": self.id,
            "ceco_id_erp": self.ceco_id_erp,
            "nombre": self.nombre,
            "descripcion": self.descripcion
        }
