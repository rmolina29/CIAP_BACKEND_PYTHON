from pydantic import BaseModel


# Define el modelo Pydantic
class Archivo(BaseModel):
    ceco_id_erp: str
    nombre: str
    descripcion: str