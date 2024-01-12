from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
import uvicorn
from app.parametros.gerencia import gerencia_router as router_g
from app.parametros.direccion import direccion_router as router_d
from app.parametros.ceco import ceco_router as router_c
from app.parametros.cliente import cliente_router as router_cli
from app.parametros.estado import estado_router as router_e
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
app.include_router(
    router_c.ceco, prefix="/fts/parametros/ceco"
)
app.include_router(
    router_cli.cliente, prefix="/fts/parametros/cliente"
)
app.include_router(
    router_e.estado, prefix="/fts/parametros/estado"
)


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
