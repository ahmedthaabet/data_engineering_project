from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
    try:
        # Connect to PostgreSQL server (without specifying database)
        conn = psycopg2.connect(
            host='pgdatabase',
            user='root',
            password='root',
            port=5432
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'portfolio_db'")
        exists = cursor.fetchone()
        
        if not exists:
            # Create database
            cursor.execute('CREATE DATABASE portfolio_db')
            print("Database 'portfolio_db' created successfully")
        else:
            print("Database 'portfolio_db' already exists")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating database: {e}")

def save_to_db(cleaned, table_name):
    create_database()
    # postgresql://username:password@container_name:port/database_name
    engine = create_engine('postgresql://root:root@pgdatabase:5432/portfolio_db')
    if(engine.connect()):
        print('Connected to Database')
        try:
            print('Writing cleaned dataset to database')
            cleaned.to_sql(table_name, con=engine, if_exists='replace')
            print('Done writing to database')
        except ValueError as vx:
            print('Cleaned Table already exists.')
        except Exception as ex:
            print(ex)
    else:
        print('Failed to connect to Database')