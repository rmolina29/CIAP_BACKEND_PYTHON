import sqlalchemy
from databases import Database
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

load_dotenv()

try:
    DATABASE_URL = f"{os.getenv('DB_MODE')}://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}?charset=utf8mb4&collation=utf8mb4_general_ci"
    database = Database(DATABASE_URL)
    
    engine = sqlalchemy.create_engine(
        DATABASE_URL,
        encoding='utf-8',
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=3600
    )

    Session = sessionmaker(bind=engine,autoflush=True,autocommit=True)
    session = Session()
    Base = declarative_base()
    
    # logging.basicConfig()
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
except SQLAlchemyError as e:
    raise RuntimeError(f"Error en la conexión a la base de datos: {str(e)}")

