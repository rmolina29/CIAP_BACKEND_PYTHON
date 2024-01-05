import sqlalchemy
from databases import Database
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import os
import logging

load_dotenv()

try:
    DATABASE_URL = f"{os.getenv("DB_MODE")}://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv('DB_DATABASE')}?charset=utf8"
    database = Database(DATABASE_URL)
    # metadata = sqlalchemy.MetaData()
    engine = sqlalchemy.create_engine(DATABASE_URL,
                                      encoding='utf-8',
                                      pool_size=5,
                                      max_overflow=10)

    Session = sessionmaker(bind=engine)
    session = Session()
    Base = declarative_base()
    
    # logging.basicConfig()
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
except SQLAlchemyError as e:
    error_message = f"Error en la conexión a la base de datos: {str(e)}"
    print(error_message)
    JSONResponse(content={"error": error_message}, status_code=500)

