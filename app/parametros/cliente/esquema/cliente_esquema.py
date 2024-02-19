from pydantic import BaseModel


# Define el modelo Pydantic
class Archivo(BaseModel):
    cliente_id_erp: str
    razon_social: str
    identificacion: int