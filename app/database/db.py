import sqlalchemy
from databases import Database
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

load_dotenv()

try:
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}?options=-csearch_path%3Ddata"
    
    # Crear instancia de Database
    database = Database(DATABASE_URL)

    # Crear el motor SQLAlchemy con configuraciones de pool
    engine = sqlalchemy.create_engine(
        DATABASE_URL,
        pool_size=5,
        pool_recycle=3600
    )

    # Configurar sesión SQLAlchemy
    Session = sessionmaker(bind=engine, autoflush=True, autocommit=False)  
    session = Session()
    
    # Crear la clase base declarativa
    Base = declarative_base()
    
except SQLAlchemyError as e:
    raise RuntimeError(f"Error en la conexión a la base de datos: {str(e)}")
