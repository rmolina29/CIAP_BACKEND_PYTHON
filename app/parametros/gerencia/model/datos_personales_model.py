from sqlalchemy import Column, Integer,ForeignKey
from sqlalchemy.orm import relationship
# from sqlalchemy.ext.declarative import declarative_base
from app.database.db import Base
from sqlalchemy import SmallInteger
# Base = declarative_base()
from sqlalchemy.dialects.postgresql import INTEGER
class UsuarioDatosPersonales(Base):
    __tablename__ = "usuario_datos_personales"

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_usuario = Column(Integer, ForeignKey('usuario_auth.id'), nullable=False)
    identificacion = Column(Integer, nullable=False)
    estado = Column(Integer, default=1)
    
    usuario_auth = relationship("UsuarioAuth", back_populates="usuario_datos_personales")

    def __init__(self, id, id_usuario, identificacion, estado):
        self.id = id
        self.id_usuario = id_usuario
        self.identificacion = identificacion
        self.estado = estado

    def to_dict(self):
        return {
            "id_usuario": self.id_usuario,
            "identificacion": self.identificacion
        }
