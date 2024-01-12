from sqlalchemy import Column, Integer, Text
from app.database.db import Base


class ProyectoCliente(Base):
    
    __tablename__ = "proyecto_cliente"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cliente_id_erp = Column(Text, nullable=False, unique=True)
    razon_social = Column(Text, unique=True, nullable=True)
    identificacion = Column(Integer,nullable = True)
   

    def __init__(self, id, cliente_id_erp, razon_social,identificacion):
        self.id = id
        self.cliente_id_erp = cliente_id_erp
        self.razon_social = razon_social
        self.identificacion = identificacion


    def to_dict(self):
        return {
            "id": self.id,
            "cliente_id_erp": self.cliente_id_erp,
            "razon_social": self.razon_social,
            "identificacion": self.identificacion
        }
