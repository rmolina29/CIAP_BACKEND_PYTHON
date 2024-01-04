from fastapi import APIRouter
from fastapi.responses import JSONResponse


gerencia = APIRouter()

@gerencia.get('/obtener')
def obtener_gerencia():
    try:
        data = {"message": "Hello, FastAPI prueba "}
        status_code = 200
        return JSONResponse(content=data, status_code=status_code)
    except Exception as e:
        print(e)
        


