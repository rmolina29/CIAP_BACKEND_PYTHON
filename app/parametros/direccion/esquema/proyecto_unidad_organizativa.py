from pydantic import BaseModel


# Define el modelo Pydantic
class Archivo(BaseModel):
    unidad_organizativa_id_erp: str
    nombre: str
    unidad_gerencia_id_erp: str