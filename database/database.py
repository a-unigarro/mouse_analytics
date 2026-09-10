import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

# Load the variables from the .env file
load_dotenv()

# Database configuration
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DATABASE_URL=f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLAlchemy objects
Base = declarative_base()
SessionLocal = sessionmaker(
                            autocommit=False, 
                            autoflush=False
                            )
engine = None

# Database initialization
def configure_database():
    global engine
    engine = create_engine(DATABASE_URL)
    SessionLocal.configure(bind=engine)
    return engine


