from pydantic import BaseModel, constr, ValidationError, validator

# Define el modelo Pydantic
class Archivo(BaseModel):
    unidad_gerencia_id_erp: str
    nombre: str
    NIT: constr(strip_whitespace=True)

    @validator("NIT")
    def nit_is_valid(cls, v):
        # Validar que el NIT es numérico y tiene 11 dígitos
        if not v.isdigit() or len(v) != 11:
            raise ValueError("El NIT debe contener solo 11 dígitos numéricos.")
        return v