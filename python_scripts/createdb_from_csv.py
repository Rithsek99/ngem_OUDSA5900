import pandas as pd
import sqlite3
import csv
import time
import io
from datetime import date
from datetime import timedelta

# Death_case
def createdb_from_csv_death(death_csv_file, positive_csv_file, db_file, aggregated_csv_file):
    # Open a connection to the database (create if not exist)
    conn = sqlite3.connect(db_file)
    # Create a cursor object to execute SQL commands
    cur = conn.cursor()

    # Create a temp table to store death data
    cur.execute('''
        CREATE TABLE IF NOT EXISTS temp_death_table (
            fecha_resultado TEXT,
            departamento TEXT,
            provincia TEXT,
            distrito TEXT
        );
    ''')
    
    # Create a temp table to store positive data
    cur.execute('''
        CREATE TABLE IF NOT EXISTS temp_positive_table (
            fecha_resultado TEXT,
            departamento TEXT,
            provincia TEXT,
            distrito TEXT
        );
    ''')
    
    # Insert data into death table
    feature = ['fecha_resultado', 'departamento', 'provincia', 'distrito']
    death_data = pd.read_csv(death_csv_file)
    death_data = death_data[feature]
    death_data.to_sql('temp_death_table', conn, if_exists='append', index=False)
    
    # Insert data into positive table
    positive_data = pd.read_csv(positive_csv_file)
    positive_data = positive_data.rename(columns={'departmento': 'departamento'})
    positive_data = positive_data[feature]
    positive_data.to_sql('temp_positive_table', conn, if_exists='append', index=False)
    
    # tables for death case geographically 
    # Create table for district-level aggregation
    cur.execute('''
        CREATE TABLE IF NOT EXISTS distrito_death_cases (
            fecha_resultado TEXT,
            departamento TEXT,
            provincia TEXT,
            distrito TEXT, 
            num_death_cases INTEGER
        );
    ''')
    
    #Create table for province-level aggregation
    cur.execute('''
        CREATE TABLE IF NOT EXISTS province_death_cases (
            fecha_resultado TEXT,
            departamento TEXT,
            provincia TEXT,
            num_death_cases INTEGER
        );
    ''')
    
    # CREATE table for department-level aggregation
    cur.execute('''
        CREATE TABLE IF NOT EXISTS department_death_cases (
            fecha_resultado TEXT,
            departamento TEXT,
            num_death_cases INTEGER
        );
    ''')
    
    # CREATE table for country-level aggregation
    cur.execute('''
        CREATE TABLE IF NOT EXISTS country_death_cases (
            fecha_resultado TEXT,
            num_death_cases INTEGER
        );
    ''')
    
    # tables for positive case geographically
    # Create table for district-level aggregation
    cur.execute('''
        CREATE TABLE IF NOT EXISTS distrito_positive_cases (
            fecha_resultado TEXT,
            departamento TEXT,
            provincia TEXT,
            distrito TEXT, 
            num_positive_cases INTEGER
        );
    ''')
    
    #Create table for province-level aggregation
    cur.execute('''
        CREATE TABLE IF NOT EXISTS province_positive_cases (
            fecha_resultado TEXT,
            departamento TEXT,
            provincia TEXT,
            num_positive_cases INTEGER
        );
    ''')
    
    # CREATE table for department-level aggregation
    cur.execute('''
        CREATE TABLE IF NOT EXISTS department_positive_cases (
            fecha_resultado TEXT,
            departamento TEXT,
            num_positive_cases INTEGER
        );
    ''')
    
    # CREATE table for country-level aggregation
    cur.execute('''
        CREATE TABLE IF NOT EXISTS country_positive_cases (
            fecha_resultado TEXT,
            num_positive_cases INTEGER
        );
    ''')
    
    # Insert into death tables base on aggregation
    cur.execute('''
        INSERT INTO distrito_death_cases(fecha_resultado, departamento, provincia, distrito, num_death_cases)
        SELECT fecha_resultado, departamento, provincia, distrito, COUNT(*)
        FROM temp_death_table
        GROUP BY fecha_resultado, departamento, provincia, distrito
        ORDER BY fecha_resultado, departamento, provincia, distrito
    ''')
    
    cur.execute('''
        INSERT INTO province_death_cases(fecha_resultado, departamento, provincia, num_death_cases)
        SELECT fecha_resultado, departamento, provincia, COUNT(*)
        FROM temp_death_table
        GROUP BY fecha_resultado, departamento, provincia
        ORDER BY fecha_resultado, departamento, provincia
    ''')

    cur.execute('''
        INSERT INTO department_death_cases(fecha_resultado, departamento, num_death_cases)
        SELECT fecha_resultado, departamento, COUNT(*)
        FROM temp_death_table
        GROUP BY fecha_resultado, departamento
        ORDER BY fecha_resultado, departamento
    ''')
    
    cur.execute('''
        INSERT INTO country_death_cases(fecha_resultado, num_death_cases)
        SELECT fecha_resultado, COUNT(*)
        FROM temp_death_table
        GROUP BY fecha_resultado
        ORDER BY fecha_resultado
    ''')
    
    # Insert into positive tables
    cur.execute('''
        INSERT INTO distrito_positive_cases(fecha_resultado, departamento, provincia, distrito, num_positive_cases)
        SELECT fecha_resultado, departamento, provincia, distrito, COUNT(*)
        FROM temp_positive_table
        GROUP BY fecha_resultado, departamento, provincia, distrito
        ORDER BY fecha_resultado, departamento, provincia, distrito
    ''')
    
    cur.execute('''
        INSERT INTO province_positive_cases(fecha_resultado, departamento, provincia, num_positive_cases)
        SELECT fecha_resultado, departamento, provincia, COUNT(*)
        FROM temp_positive_table
        GROUP BY fecha_resultado, departamento, provincia
        ORDER BY fecha_resultado, departamento, provincia
    ''')

    cur.execute('''
        INSERT INTO department_positive_cases(fecha_resultado, departamento, num_positive_cases)
        SELECT fecha_resultado, departamento, COUNT(*)
        FROM temp_positive_table
        GROUP BY fecha_resultado, departamento
        ORDER BY fecha_resultado, departamento
    ''')
    
    cur.execute('''
        INSERT INTO country_positive_cases(fecha_resultado, num_positive_cases)
        SELECT fecha_resultado, COUNT(*)
        FROM temp_positive_table
        GROUP BY fecha_resultado
        ORDER BY fecha_resultado
    ''')
    
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    file_names = ['Positive_Cases_', 'Deaths_', 'DHV_']
    # Index for iterating through file name list/features and gets current time.
    file_index = 0
    
    today = date.today()
    today_str = str(today)
    positive_csv_file = file_names[file_index] + today_str + ".csv"
    death_csv_file = file_names[file_index+1] + today_str + ".csv"
    db_file = 'peru_data.db'
    aggregated_csv_file = file_names[file_index] + today_str + "_aggregated.csv"
    
    createdb_from_csv_death(death_csv_file, positive_csv_file, db_file, aggregated_csv_file)

