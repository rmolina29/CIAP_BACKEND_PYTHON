from click import File
import pandas as pd
from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse
from app.parametros.direccion.direccion_servicio import Direccion

direccion = APIRouter(tags=["Direccion"])


@direccion.post("/subir-archivo")
async def subir_archivo(file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file)
        # Imprimir las columnas reales del DataFrame
        selected_columns = ["ID Dirección (ERP)", "Dirección", "ID Gerencia (ERP)"]

        selected_data = df[selected_columns]

        # Cambiar los nombres de las columnas
        selected_data = selected_data.rename(
            columns={
                "ID Dirección (ERP)": "unidad_organizativa_id_erp",
                "Dirección": "nombre",
                "ID Gerencia (ERP)": "unidad_gerencia_id_erp",
            }
        )

        # Convertir a un diccionario en formato de registros
        result_dict = selected_data.to_dict(orient="records")

        direccion = Direccion(result_dict)

        enviar_informacion = direccion.transacciones()

        # return enviar_informacion
        return JSONResponse(content=enviar_informacion, media_type="application/json")
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
