from sqlalchemy import create_engine, Column, Integer, Float, String, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
import csv

# Define your database connection URL
db_host = "0.0.0.0" 
db_port = "5432"       
db_name = "etl-database"   
db_user = "postgres"  
db_password = "root"  
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create a SQLAlchemy engine
engine = create_engine(db_url)

# Define your data model using SQLAlchemy's declarative_base
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

# Create the database tables (if they don't exist)
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

csv_file = 'raw_data/medicalmalpractice.csv'

try:
    with open(csv_file, 'r', newline='') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            stock_price = Lifestyle(
                amount=int(row[0]),
                severity=int(row[1]),
                age=int(row[2]),
                private_attorney=int(row[3]),
                marital_status=int(row[4]),
                specialty=row[5],
                insurance=row[6],
                gender=row[7]
            )
            session.add(stock_price)
    
    session.commit()
    print("CSV data has been transferred to the PostgreSQL table using SQLAlchemy ORM.")

except Exception as e:
    print(f"Error: {e}")
finally:
    session.close()