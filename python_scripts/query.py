import pandas as pd
import sqlite3


conn = sqlite3.connect('peru_data.db')
    # Create a cursor object to execute SQL commands
cur = conn.cursor()

# Create aggregated table for district-level death and positive cases
cur.execute('''
            CREATE TABLE IF NOT EXISTS distrito_death_positive_cases(
                fecha_resultado TEXT,
                departamento TEXT,
                provincia TEXT,
                distrito TEXT,
                num_death_cases INTEGER,
                num_positive_cases INTEGER
            );
            ''')

# Create aggregated table for province-level death and positive cases
cur.execute('''
            CREATE TABLE IF NOT EXISTS province_death_positive_cases(
                fecha_resultado TEXT,
                departamento TEXT,
                provincia TEXT,
                num_death_cases INTEGER,
                num_positive_cases INTEGER 
            );
            ''')

# Create aggregated table for department-level death and positive cases
cur.execute('''
            CREATE TABLE IF NOT EXISTS department_death_positive_cases(
            fecha_resultado TEXT,
            departamento TEXT,
            num_death_cases INTEGER,
            num_positive_cases INTEGER
            );  
            ''')

# Create aggregated table for country-level death and positive cases
cur.execute('''
            CREATE TABLE IF NOT EXISTS country_death_positive_cases(
                fecha_resultado TEXT,
                num_death_cases INTEGER,
                num_positive_cases INTEGER
            );
            ''')

# Aggregate district-level death and positive cases and insert into table
cur.execute('''
            INSERT INTO distrito_death_positive_cases(fecha_resultado, departamento, provincia, distrito, num_death_cases, num_positive_cases) 
            
            SELECT * FROM (
                SELECT t1.fecha_resultado, t1.departamento, t1.provincia, t1.distrito, COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM distrito_death_cases t1
                LEFT JOIN distrito_positive_cases t2 ON t1.fecha_resultado = t2.fecha_resultado AND t1.departamento = t2.departamento AND t1.provincia = t2.provincia AND t1.distrito = t2.distrito
                
                UNION ALL
                
                SELECT t2.fecha_resultado, t2.departamento, t2.provincia, t2.distrito, COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM distrito_positive_cases t2
                LEFT JOIN distrito_death_cases t1 ON t1.fecha_resultado = t2.fecha_resultado AND t1.departamento = t2.departamento AND t1.provincia = t2.provincia AND t1.distrito = t2.distrito
                WHERE t1.fecha_resultado IS NULL AND t1.departamento IS NULL AND t1.provincia IS NULL AND t1.distrito IS NULL
            ) subquery
            ORDER BY fecha_resultado asc, departamento asc, provincia asc, distrito asc;
            ''')

# Aggregate province-level death and positive cases and insert into table
cur.execute('''
            INSERT INTO province_death_positive_cases(fecha_resultado, departamento, provincia, num_death_cases, num_positive_cases) 
            
            SELECT * FROM (
                SELECT t1.fecha_resultado, t1.departamento, t1.provincia, COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM province_death_cases t1
                LEFT JOIN province_positive_cases t2 ON t1.fecha_resultado = t2.fecha_resultado AND t1.departamento = t2.departamento AND t1.provincia = t2.provincia 
                
                UNION ALL
                
                SELECT t2.fecha_resultado, t2.departamento, t2.provincia, COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM province_positive_cases t2
                LEFT JOIN province_death_cases t1 ON t1.fecha_resultado = t2.fecha_resultado AND t1.departamento = t2.departamento AND t1.provincia = t2.provincia
                WHERE t1.fecha_resultado IS NULL AND t1.departamento IS NULL AND t1.provincia IS NULL 
            ) subquery
            ORDER BY fecha_resultado asc, departamento asc, provincia asc;
            ''')
# Aggregate department-level death and positive cases and insert into table
cur.execute('''
            INSERT INTO department_death_positive_cases(fecha_resultado, departamento, num_death_cases, num_positive_cases) 
            
            SELECT * FROM (
                SELECT t1.fecha_resultado, t1.departamento,COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM department_death_cases t1
                LEFT JOIN department_positive_cases t2 ON t1.fecha_resultado = t2.fecha_resultado AND t1.departamento = t2.departamento
                
                UNION ALL
                
                SELECT t2.fecha_resultado, t2.departamento, COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM department_positive_cases t2
                LEFT JOIN department_death_cases t1 ON t1.fecha_resultado = t2.fecha_resultado AND t1.departamento = t2.departamento
                WHERE t1.fecha_resultado IS NULL AND t1.departamento IS NULL
            ) subquery
            ORDER BY fecha_resultado asc, departamento asc;
            ''')

# Aggregate country-level death and positive cases and insert into table
cur.execute('''
            INSERT INTO country_death_positive_cases(fecha_resultado, num_death_cases, num_positive_cases) 
            
            SELECT * FROM (
                SELECT t1.fecha_resultado, COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM country_death_cases t1
                LEFT JOIN country_positive_cases t2 ON t1.fecha_resultado = t2.fecha_resultado
                
                UNION ALL
                
                SELECT t2.fecha_resultado, COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM country_positive_cases t2
                LEFT JOIN country_death_cases t1 ON t1.fecha_resultado = t2.fecha_resultado
                WHERE t1.fecha_resultado IS NULL 
            ) subquery
            ORDER BY fecha_resultado asc;
            ''')

conn.commit()
conn.close()