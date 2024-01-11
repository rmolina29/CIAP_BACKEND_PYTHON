from click import File
from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse
from app.parametros.ceco.ceco_servicio import Ceco

ceco = APIRouter(tags=["Ceco"])


@ceco.post("/subir-archivo")
async def subir_archivo(file: UploadFile = File(...)):
    try:
        ceco = Ceco(file)
        enviar_informacion = ceco.transacciones()
        return JSONResponse(content=enviar_informacion, media_type="application/json")
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
