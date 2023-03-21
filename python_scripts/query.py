import pandas as pd
import sqlite3

conn = sqlite3.connect('peru_data.db')
    # Create a cursor object to execute SQL commands
cur = conn.cursor()

query = '''select * from department_positive_cases;'''


'''department_death_cases --> t1
department_positive_cases--> t2'''

#department_death_positive_cases('fechat_resultado', 'departamento', 'num_death_cases', 'num_positive_cases')
# query = '''
#             SELECT t1.fecha_resultado, t1.departamento,t1.num_death_cases, t2.num_positive_cases
#             FROM department_death_cases t1
#             LEFT JOIN department_positive_cases t2 ON t1.fecha_resultado = t2.fecha_resultado AND t1.departamento = t2.departamento
#             UNION ALL
#             SELECT t2.fecha_resultado, t2.departamento, t1.num_death_cases, t2.num_positive_cases
#             FROM department_positive_cases t2
#             LEFT JOIN department_death_cases t1 ON t2.fecha_resultado = t1.fecha_resultado AND t2.departamento = t1.departamento
#             WHERE t1.fecha_resultado IS NULL;
#             '''
# cur.execute(query)
# result = cur.fetchmany(10)
cur.execute('''
            CREATE TABLE IF NOT EXISTS department_death_positive_cases(
            fecha_resultado TEXT,
            departamento TEXT,
            num_death_cases INTEGER,
            num_positive_cases INTEGER
            );  
            ''')

cur.execute('''
            INSERT INTO department_death_positive_cases(fecha_resultado, departamento, num_death_cases, num_positive_cases) 
            
            SELECT * FROM (
                SELECT t1.fecha_resultado, t1.departamento,COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM department_death_cases t1
                LEFT JOIN department_positive_cases t2 ON t1.fecha_resultado = t2.fecha_resultado AND t1.departamento = t2.departamento
                UNION ALL
                SELECT t2.fecha_resultado, t2.departamento, COALESCE(t1.num_death_cases, 0), COALESCE(t2.num_positive_cases, 0)
                FROM department_positive_cases t2
                LEFT JOIN department_death_cases t1 ON t2.fecha_resultado = t1.fecha_resultado AND t2.departamento = t1.departamento
                WHERE t1.fecha_resultado IS NULL AND t1.departamento IS NULL
            ) AS combined
            
            ORDER BY combined.fecha_resultado
            ''')
conn.commit()
conn.close()