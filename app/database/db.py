import sqlalchemy
from databases import Database

from dotenv import load_dotenv
import os

load_dotenv()
               
DATABASE_URL = f"{os.getenv("DB_MODE")}://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv('DB_DATABASE')}?charset=utf8"




database = Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

engine = sqlalchemy.create_engine(DATABASE_URL, encoding='utf-8')
