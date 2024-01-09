from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
import uvicorn
from app.parametros.gerencia import gerencia_router as router_g
from app.parametros.direccion import direccion_router as router_d

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
app.include_router(
    router_g.gerencia, prefix="/fts/parametros/gerencia"
)
app.include_router(
    router_d.direccion, prefix="/fts/parametros/direccion"
)
# app.include_router(item.router, prefix="/items", tags=["items"])

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
