from sqlalchemy import update
from sqlalchemy.orm import sessionmaker
from db_handler import get_engine, get_orm_model

engine = get_engine()

Lifestyle = get_orm_model(engine)

Session = sessionmaker(bind=engine)
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
