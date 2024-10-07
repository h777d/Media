import pandas as pd
import sqlite3
import argparse
import os
import logging


def load_csv(file_path):
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist")
        df = pd.read_csv(file_path)
        logging.info(f"Successfully loaded file: {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading file {file_path}: {e}")
        raise


def db_info(db_path):
    try:
        connect = sqlite3.connect(db_path)
        cursor = connect.cursor()
        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table';")
        table_count = cursor.fetchone()[0]
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        logging.info(f"Total tables in the database: {table_count}")
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table[0]})")
            column_info = cursor.fetchall()
            column_names = [info[1] for info in column_info]
            logging.info(f"Table {table[0]} columns: {column_names}")
        
        df = pd.read_sql_query(f"SELECT * from report_date", connect)
        connect.close()
        logging.info("Extra Step: Database info retrieved from database using SQL inquiry successfully")
        return df
    except Exception as e:
        logging.error(f"Error retrieving database info: {e}")
        raise

