from fastapi import FastAPI
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


app.include_router(
    router.gerencia, prefix="/parametros/gerencia", tags=["gerencia"]
)
# app.include_router(item.router, prefix="/items", tags=["items"])

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
