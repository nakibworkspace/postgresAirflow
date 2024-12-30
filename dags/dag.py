from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime , timedelta

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by instantiating the Postgres Operator

with DAG(
    dag_id= 'postgres_operator_dag',
    start_date= datetime(2023,1,1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )

    get_all_pets = PostgresOperator(task_id="get_all_pets", sql="SELECT * FROM pet;")

    #get pets by date range
    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        runtime_parameters={'statement_timeout': '3000ms'},
    )

    # Delete pets by type
    delete_by_type= PostgresOperator(
        task_id= "delete_by_type",
        sql= "DELETE FROM pet WHERE pet_type= %(pet_type)s;",
        parameters={"pet_type":"hamster"},
    )

    #Delete pets by owner
    delete_by_owner= PostgresOperator(
        task_id='delete_by_owner',
        sql='DELETE FROM pet WHERE OWNER= %(owner)s;',
        parameters={'owner':'phil'},
    )

    # Update pet information
    update_pet= PostgresOperator(
        task_id='update_pet',
        sql='''UPDATE pet
        SET owner= %(new_owner)s
        WHERE name= %(pet_name)s;''',
        parameters= {'new_owner':'john', 'pet_name':'max'}
    )

    #Query specefic pet type
    get_pet_by_type=PostgresOperator(
        task_id='get_pet_by_type',
        sql='SELECT FROM pet WHERE pet_type= %(pet_type)s;',
        parameters={'pet_type':'dogs'},
    )

    # Count pets by type
    count_pet_by_type= PostgresOperator(
        task_id= 'count_by_type',
        sql= """
        SELECT pet_type, COUNT(*) as pet_count 
        FROM pet 
        GROUP BY pet_type;
        """,
    )



    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
    get_birth_date >> [delete_by_type, delete_by_owner]
    delete_by_type >> update_pet
    update_pet >> [get_pet_by_type, count_pet_by_type]
