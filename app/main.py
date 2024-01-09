from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
import uvicorn
from app.parametros.gerencia import gerencia_router as router

# from tortoise.contrib.fastapi import register_tortoise
# from app.config import project
from app.database.db import database

# from app.utils import load_app

app = FastAPI(debug=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Crea un enrutador para tus rutas con el prefijo "/fts"
fts_router = APIRouter(prefix="/fts")


# Agrega tus rutas al enrutador con el prefijo
@fts_router.get("/hello")
async def fts_some_path():
    return {"message": "Hello from /hello"}

@fts_router.get("/parametros/gerencia")
async def fts_some_path():
     return {"message": "Subir Archivos"}



app.include_router(fts_router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
