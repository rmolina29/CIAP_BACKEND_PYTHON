from click import File
from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse
from app.parametros.estado.estado_servicio import Estado

estado = APIRouter(tags=["Estado"])

@estado.post("/subir-archivo")
async def subir_archivo(file: UploadFile = File(...)):
    try:
        estado = Estado(file)
        enviar_informacion = estado.transacciones()
        return JSONResponse(content=enviar_informacion, media_type="application/json")
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)