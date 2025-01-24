import pymysql
pymysql.install_as_MySQLdb()

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

import time
from sqlalchemy.exc import OperationalError

def wait_for_db(engine, max_retries=5, delay=5):
    for attempt in range(max_retries):
        try:
            with engine.connect() as connection:
                print(f"Successfully connected to database on attempt {attempt + 1}")
                return True
        except OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Database connection attempt {attempt + 1} failed. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise e
    return False

try:
    # Load environment variables
    load_dotenv()

    # Source database credentials
    db_connection = os.getenv('DB_CONNECTION')
    db_host = os.getenv('DB_HOST')
    db_port = int(os.getenv('DB_PORT'))
    db_database = os.getenv('DB_DATABASE')
    db_username = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')

    # Local MySQL database credentials
    local_db_connection = os.getenv('LOCAL_DB_CONNECTION')
    local_db_host = os.getenv('LOCAL_DB_HOST')
    local_db_port = int(os.getenv('LOCAL_DB_PORT'))
    local_db_database = os.getenv('LOCAL_DB_DATABASE')
    local_db_username = os.getenv('LOCAL_DB_USERNAME')
    local_db_password = os.getenv('LOCAL_DB_PASSWORD')  # Replace with your local MySQL root password

    # Source database URL
    db_url = f"{db_connection}+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}"
    print(f"DB_CONNECTION: {db_connection}")
    print(f"DB_HOST: {db_host}")
    print(f"DB_PORT: {db_port}")
    print(f"DB_DATABASE: {db_database}")
    print(f"DB_USERNAME: {db_username}")
    print(f"DB_PASSWORD: {db_password}")
    print(f"Source DB URL: {db_url}")

    # Local database URL
    local_db_url = f"{local_db_connection}+pymysql://{local_db_username}:{local_db_password}@{local_db_host}:{local_db_port}/{local_db_database}"
    print(f"LOCAL_DB_CONNECTION: {local_db_connection}")
    print(f"LOCAL_DB_HOST: {local_db_host}")
    print(f"LOCAL_DB_PORT: {local_db_port}")
    print(f"LOCAL_DB_DATABASE: {local_db_database}")
    print(f"LOCAL_DB_USERNAME: {local_db_username}")
    print(f"LOCAL_DB_PASSWORD: {local_db_password}")
    print(f"Local DB URL: {local_db_url}")

    # Create engines for source and local databases
    try:
        source_engine = create_engine(db_url)
        local_engine = create_engine(local_db_url)

        # Wait for databases to be ready
        print("Waiting for source database connection...")
        wait_for_db(source_engine)
        print("Waiting for local database connection...")
        wait_for_db(local_engine)
    except Exception as e:
        raise Exception(f"Error creating database engines: {e}")

    # Extract data from source database
    try:
        query = """
        SELECT 
            u.id AS user_id,
            u.first_name,
            u.middle_name,
            u.last_name,
            u.birthday,
            u.male_female,
            cst.civil_status_type AS civil_status,
            COALESCE(a.appointment_count, 0) AS total_appointments
        FROM 
            users u
        LEFT JOIN 
            civil_status_types cst ON u.civil_status_id = cst.id
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS appointment_count
            FROM appointments
            GROUP BY user_id
        ) a ON u.id = a.user_id
        """
        
        data = pd.read_sql(query, source_engine)
        print("Data extracted successfully!")
    except Exception as e:
        raise Exception(f"Error extracting data from source database: {e}")

    # Transform data
    try:
        data['full_name'] = data['first_name'] + ' ' + data['middle_name'].fillna('') + ' ' + data['last_name']
        data['age'] = data['birthday'].apply(lambda x: datetime.now().year - x.year if pd.notnull(x) else None)
        data['gender'] = data['male_female'].apply(lambda x: 'Male' if x == 1 else 'Female' if x == 0 else 'Unknown')
        data_filtered = data[['full_name', 'age', 'gender', 'civil_status', 'total_appointments']]
        print("Data transformed successfully!")
    except Exception as e:
        raise Exception(f"Error transforming data: {e}")

    # Load transformed data into local MySQL database
    try:
        data_filtered.to_sql('transformed_users', local_engine, if_exists='replace', index=False)
        print("Data loaded successfully into the local MySQL database!")
    except Exception as e:
        raise Exception(f"Error loading data into local MySQL database: {e}")

except Exception as main_error:
    print(f"An error occurred during the ETL process: {main_error}")
