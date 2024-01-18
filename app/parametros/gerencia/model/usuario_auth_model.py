from sqlalchemy import Column, Integer,ForeignKey
from sqlalchemy.dialects.mysql import TINYINT
# from sqlalchemy.ext.declarative import declarative_base
from app.database.db import Base
from sqlalchemy.orm import relationship
# Base = declarative_base()


class UsuarioAuth(Base):
    
    __tablename__ = "usuario_auth"

    id = Column(Integer, primary_key=True, autoincrement = True)
    estado = Column(
        TINYINT(unsigned=True), default=1
    ) 
    
    # usuario_datos_personales = relationship("UsuarioDatosPersonales",ForeignKey('usuario_auth.id'), back_populates="usuario_datos_personales")

    def __init__(self, id,estado):
        self.id = id
        self.estado = estado

    
    def to_dict(self):
        return {
            "id": self.id,
            "estado":self.estado
        }

