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
        selected_data["unidad_organizativa_id_erp"] = selected_data["unidad_organizativa_id_erp"].str.lower()
        selected_data["nombre"] = selected_data["nombre"].str.lower()

        # Convertir a un diccionario en formato de registros
        # result_dict = selected_data.drop_duplicates(subset=['unidad_organizativa_id_erp','nombre'], keep='first').to_dict(orient="records")

        duplicados_unidad_erp = selected_data.duplicated(subset='unidad_organizativa_id_erp', keep='first')
        duplicados_nombre = selected_data.duplicated(subset='nombre', keep='first')

        # Filtrar DataFrame original
        resultado = selected_data[~(duplicados_unidad_erp | duplicados_nombre)].to_dict(orient='records')

        direccion = Direccion(resultado)
        

        enviar_informacion = direccion.transacciones()

        return JSONResponse(content=enviar_informacion, media_type="application/json")
    except Exception as e:
        print(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)
