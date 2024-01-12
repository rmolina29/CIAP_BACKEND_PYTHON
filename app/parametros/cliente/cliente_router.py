from click import File
from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse
from app.parametros.cliente.cliente_servicio import Cliente

cliente = APIRouter(tags=["Cliente"])

@cliente.post("/subir-archivo")
async def subir_archivo(file: UploadFile = File(...)):
    try:
        cliente = Cliente(file)
        enviar_informacion = cliente.transacciones()
        return JSONResponse(content=enviar_informacion, media_type="application/json")
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
