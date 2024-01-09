from typing import List
from app.parametros.direccion.esquema.proyecto_unidad_organizativa import Archivo


class Direccion:
    def __init__(self,data:List[Archivo]) -> None:
        self.__direcion_excel = data
    
    def transacciones(self):
        return self.__direcion_excel