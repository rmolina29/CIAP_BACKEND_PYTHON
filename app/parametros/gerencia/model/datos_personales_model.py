from sqlalchemy import Column, Integer,ForeignKey
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship
# from sqlalchemy.ext.declarative import declarative_base
from app.database.db import Base

# Base = declarative_base()

class UsuarioDatosPersonales(Base):
    
    __tablename__ = "usuario_datos_personales"

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_usuario = Column(Integer, nullable=False)
    identificacion = Column(Integer, nullable=False)
    estado = Column(
        TINYINT(unsigned=True), default=1
    ) 
    
    usuario_auth = relationship("UsuarioAuth",ForeignKey('usuario_auth.id'), back_populates="usuario_datos_personales")
    
    def __init__(self, id, id_usuario, identificacion,estado):
        self.id = id
        self.id_usuario = id_usuario
        self.identificacion = identificacion
        self.estado = estado

    
    def to_dict(self):
        return {
            "id_usuario": self.id_usuario,
            "identificacion":self.identificacion
        }

