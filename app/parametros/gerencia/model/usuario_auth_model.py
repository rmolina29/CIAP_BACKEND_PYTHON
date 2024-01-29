from sqlalchemy import Column, Integer,ForeignKey
# from sqlalchemy.ext.declarative import declarative_base
from app.database.db import Base
from sqlalchemy.orm import relationship
# Base = declarative_base()
from sqlalchemy import SmallInteger
from sqlalchemy.dialects.postgresql import INTEGER

class UsuarioAuth(Base):
    __tablename__ = "usuario_auth"

    id = Column(Integer, primary_key=True, autoincrement=True)
    estado = Column(Integer, default=1)
    
    # Corrige la relación bidireccional
    usuario_datos_personales = relationship("UsuarioDatosPersonales", back_populates="usuario_auth")

    def __init__(self, id, estado):
        self.id = id
        self.estado = estado
    
    def to_dict(self):
        return {
            "id": self.id,
            "estado": self.estado
        }

