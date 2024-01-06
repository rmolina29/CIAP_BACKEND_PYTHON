from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.parametros.gerencia.gerencia_servicio import Gerencia
from app.parametros.gerencia.esquema.archivo_esquema import Archivo
from typing import List

gerencia = APIRouter()


@gerencia.get("/obtener")
def obtener_gerencia():
    try:
        gerenciaModelo = Gerencia()
        informacion_gerencia = gerenciaModelo.obtener()
        status_code = 200
        return JSONResponse(content=informacion_gerencia, status_code=status_code)

    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@gerencia.post("/subir-archivo")
def subir_archivo(datosArchivo:List[Archivo]):
    try:
        gerenciaModelo = Gerencia()
        enviar_informacion = gerenciaModelo.registrar_informacion(datosArchivo)
        return JSONResponse(content=enviar_informacion, status_code=201)
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
