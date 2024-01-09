import json
from click import File
import pandas as pd
from fastapi import APIRouter,File, UploadFile
from fastapi.responses import JSONResponse
from app.parametros.gerencia.gerencia_servicio import Gerencia

gerencia = APIRouter(tags=['Gerencia'])
@gerencia.post("/subir-archivo")
async def subir_archivo(file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file)
              # Imprimir las columnas reales del DataFrame
        selected_columns = ["ERP", "Gerencia", "NIT"]
        
        selected_data = df[selected_columns]
        
        # Cambiar los nombres de las columnas
        selected_data = selected_data.rename(columns={"ERP": "unidad_gerencia_id_erp", 
                                                    "Gerencia": "nombre", 
                                                    "NIT": "NIT"})
        
        # Convertir a un diccionario en formato de registros
        result_dict = selected_data.to_dict(orient="records")

        servicio_gerencia = Gerencia(result_dict)
        
        enviar_informacion = servicio_gerencia.registrar_informacion()

        # return enviar_informacion
        return JSONResponse(content=enviar_informacion, media_type="application/json")
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
