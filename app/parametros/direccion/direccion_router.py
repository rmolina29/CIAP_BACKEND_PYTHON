from click import File
import pandas as pd
from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse
from app.parametros.direccion.direccion_servicio import Direccion

direccion = APIRouter(tags=["Direccion"])


@direccion.post("/subir-archivo")
async def subir_archivo(file: UploadFile = File(...)):
    try:
        direccion = Direccion(file)
        enviar_informacion = direccion.transacciones()
        return JSONResponse(content=enviar_informacion, media_type="application/json")
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


    def procesar_datos_excel(file):
        pass