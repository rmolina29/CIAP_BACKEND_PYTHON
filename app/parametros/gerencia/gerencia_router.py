from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.parametros.gerencia.gerencia_servicio import Gerencia

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


@gerencia.post("archivos")
def subir_archivo():
    try:
        pass
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
