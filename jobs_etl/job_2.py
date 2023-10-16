from sqlalchemy.orm import sessionmaker
from db_handler import get_engine, get_orm_model
import csv

engine = get_engine()

Lifestyle = get_orm_model(engine)

Session = sessionmaker(bind=engine)
session = Session()

csv_file = 'raw_data/medicalmalpractice.csv'

try:
    with open(csv_file, 'r', newline='') as file:
        reader = csv.reader(file)
        next(reader)  #
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
