from sqlalchemy import create_engine, Column, Integer, String, update
from sqlalchemy.orm import declarative_base, sessionmaker

db_host = "0.0.0.0"
db_port = "5432"
db_name = "etl-database"
db_user = "postgres"
db_password = "root"
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

engine = create_engine(db_url)

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

Session = sessionmaker(bind=engine)

def update_gender_column():
    update_session = Session()

    try:
        update_statement_male = update(Lifestyle).where(Lifestyle.gender == 'Male')
        update_statement_male = update_statement_male.values(gender=0)
        
        update_statement_female = update(Lifestyle).where(Lifestyle.gender == 'Female')
        update_statement_female = update_statement_female.values(gender=1)

        result_male = update_session.execute(update_statement_male)
        updated_rows_male = result_male.rowcount

        result_female = update_session.execute(update_statement_female)
        updated_rows_female = result_female.rowcount

        update_session.commit()
        print(f"{updated_rows_male} rows with 'Male' updated to 0, and {updated_rows_female} rows with 'Female' updated to 1.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        update_session.close()

update_gender_column()
