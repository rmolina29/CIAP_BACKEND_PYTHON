from sqlalchemy import Column, Integer, Text,Date,Float
from app.database.db import Base
from sqlalchemy.dialects.mysql import TINYINT

class ModeloProyectos(Base):
    
    __tablename__ = "proyecto"

    id = Column(Integer, primary_key=True, autoincrement=True)
    proyecto_id_erp = Column(Text, nullable = False, unique = True)
    nombre = Column(Text, nullable=True, unique=False)
    fecha_inicio = Column(Date, nullable=True, unique=False)
    fecha_final = Column(Date, nullable = True, unique=False)
    valor_inicial = Column(Float, nullable=True, unique = False)
    valor_final = Column(Float, nullable=True, unique = False)
    duracion = Column(Integer, nullable=True, unique = False)
    contrato = Column(Text, unique = False, nullable = True)
    estado_id = Column(Integer, unique = False, nullable = False, primary_key = True)
    objeto = Column(Text, nullable=True, unique = False)
    unidad_gerencia_id = Column(Integer, unique = False, nullable = False, primary_key = True)
    unidad_organizativa_id = Column(Integer, unique = False, nullable=False, primary_key = True)
    cliente_id = Column(Integer, unique = False, nullable=False, primary_key=True)
    ceco_id = Column(Integer, unique = False, nullable=False, primary_key=True)
    estado = Column(TINYINT(unsigned=True), default = 1)

    def __init__(self, 
            id,
            proyecto_id_erp,
            nombre,
            fecha_inicio,
            fecha_final,
            valor_inicial,
            valor_final,
            duracion,
            contrato,
            estado_id,
            objeto,
            unidad_gerencia_id,
            unidad_organizativa_id,
            cliente_id,
            ceco_id):
        
        self.id = id
        self.proyecto_id_erp = proyecto_id_erp
        self.nombre = nombre
        self.fecha_inicio = fecha_inicio
        self.fecha_final = fecha_final
        self.valor_inicial = valor_inicial
        self.valor_final = valor_final
        self.duracion = duracion
        self.contrato = contrato
        self.estado_id = estado_id
        self.objeto = objeto
        self.unidad_gerencia_id = unidad_gerencia_id
        self.unidad_organizativa_id = unidad_organizativa_id
        self.cliente_id = cliente_id
        self.ceco_id = ceco_id


    def to_dict(self):
        return {
            "id": self.id,
            "proyecto_id_erp": self.proyecto_id_erp,
            "nombre": self.nombre,
            "fecha_inicio": self.fecha_inicio,
            "fecha_final": self.fecha_final,
            "valor_inicial": self.valor_inicial,
            "valor_final": self.valor_final,
            "duracion": self.duracion,
            "contrato": self.contrato,
            "estado_id": self.estado_id,
            "objeto": self.objeto,
            "unidad_gerencia_id": self.unidad_gerencia_id,
            "unidad_organizativa_id": self.unidad_organizativa_id,
            "cliente_id": self.cliente_id,
            "ceco_id": self.ceco_id,
        }
