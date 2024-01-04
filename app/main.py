from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
import uvicorn
from app.parametros.gerencia  import gerencia_router as router
# from tortoise.contrib.fastapi import register_tortoise
# from app.config import project
from app.database.db import database
# from app.utils import load_app

app = FastAPI(debug=True)

app.add_middleware(CORSMiddleware,
                   allow_origins=['*'], allow_credentials=True,
                   allow_methods=['*'], allow_headers=['*']
                   )


app.include_router(router.gerencia, prefix="/parametros/gerencia_data", tags=["gerencia"])
# app.include_router(item.router, prefix="/items", tags=["items"])


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
    print(database)




# sub_app_names = [i.stem for i in project.SUB_APP_DIR.iterdir()]
# for sub_app_name in sub_app_names:
#     app.mount(f"/{sub_app_name}", app=load_app(sub_app_name), name=sub_app_name)


# register_tortoise(
#     app,
#     db_url=f'mysql://{project.DB_USER}:{project.DB_PSWD}@{project.DB_HOST}:3306/{project.DB_NAME}',
#     modules={"models": ["app.models"]},
#     generate_schemas=False,
#     add_exception_handlers=True,
# )