from pydantic import BaseModel

# Define el modelo Pydantic
class Archivo(BaseModel):
    estado_id_erp: str
    descripcion: str