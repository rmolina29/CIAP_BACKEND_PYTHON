from click import File
from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse
from app.proyectos.proyectos_servicio import Proyectos

proyectos = APIRouter(tags=["Proyectos"])

@proyectos.post("/subir-archivo")
async def subir_archivo(file: UploadFile = File(...)):
    try:
        proyectos = Proyectos(file)
        obtener_log_transaccion = proyectos.transacciones()
        return JSONResponse(content = obtener_log_transaccion, media_type = "application/json")
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)