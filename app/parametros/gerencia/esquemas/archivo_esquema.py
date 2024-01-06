from typing import List
from pydantic import BaseModel


# Define el modelo Pydantic
class Archivo(BaseModel):
    unidad_gerencia_id_erp: int
    nombre: str
    responsable_id: int

