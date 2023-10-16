from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

def get_engine():
    db_host = "0.0.0.0" 
    db_port = "5432"       
    db_name = "etl-database"   
    db_user = "postgres"  
    db_password = "root"  
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)


def get_orm_model(engine):
    Base = declarative_base()

    class Lifestyle(Base):
        __tablename__ = 'Lifestyle'

        id = Column(Integer, primary_key=True)
        amount = Column(Integer)
        severity = Column(Integer)
        age = Column(Integer)
        private_attorney = Column(Integer)
        marital_status = Column(Integer)
        specialty = Column(String(50))
        insurance = Column(String(50))
        gender = Column(String(50))

    Base.metadata.create_all(engine)
    return Lifestyle
