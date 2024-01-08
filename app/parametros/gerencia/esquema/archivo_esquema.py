from typing import List
from pydantic import BaseModel


# Define el modelo Pydantic
class Archivo(BaseModel):
    unidad_gerencia_id_erp: str
    nombre: str
    NIT: int