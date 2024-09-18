import psycopg2
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

#connect to the db

con = psycopg2.connect(
    host = os.getenv('HOST'),
    database = os.getenv('DATABASE'),
    user = os.getenv('USER'),
    password = os.getenv('PASSWORD')
 )


#function to get all the ids from the tutors

def get_tutors_ids():
    cur = con.cursor()
    cur.execute("select distinct respuesta from mdl_talentospilos_df_respuestas mtdr where id_pregunta = 61 and respuesta != ''")
    rows = cur.fetchall()
    cur.close()
    formatted_rows = [respuesta[0] for respuesta in rows]
    
    return formatted_rows

#function to get all the dates of the follow-ups

def get_student_follow_ups_dates(student_id):
    cur = con.cursor()
    query = f"""
    SELECT FRS.id_formulario_respuestas AS id_registro, FRS.fecha_hora_registro_respuesta AS fecha_hora_registro 
    FROM mdl_talentospilos_df_respuestas AS R 
    INNER JOIN (
        SELECT FR.id_formulario, FR.fecha_hora_registro AS fecha_hora_registro_respuesta, 
               FS.id_formulario_respuestas, FS.id_respuesta 
        FROM mdl_talentospilos_df_form_resp AS FR 
        INNER JOIN mdl_talentospilos_df_form_solu AS FS 
        ON FR.id = FS.id_formulario_respuestas 
        WHERE FR.id_formulario = 1 
        AND FR.estado = 1
    ) AS FRS 
    ON FRS.id_respuesta = R.id 
    WHERE R.respuesta = '{student_id}'
    AND R.id_pregunta = 25 
    ORDER BY FRS.fecha_hora_registro_respuesta DESC;
    """
    cur.execute(query.strip())
    rows = cur.fetchall()
    cur.close()
    
    # Format the datetime objects
    formatted_rows = [(id_registro, fecha_hora_registro.strftime('%Y-%m-%d %H:%M:%S')) for id_registro, fecha_hora_registro in rows]
    
    return formatted_rows

def get_tutor_follow_ups(tutor_id):
    cur = con.cursor()
    query = f"""
    SELECT FRS.id_formulario_respuestas AS id_registro
    FROM mdl_talentospilos_df_respuestas AS R
    INNER JOIN (
        SELECT FR.id_formulario, 
               FR.fecha_hora_registro AS fecha_hora_registro_respuesta, 
               FS.id_formulario_respuestas, 
               FS.id_respuesta
        FROM mdl_talentospilos_df_form_resp AS FR
        INNER JOIN mdl_talentospilos_df_form_solu AS FS 
        ON FR.id = FS.id_formulario_respuestas 
        WHERE FR.id_formulario = 1
          AND FR.estado = 1
    ) AS FRS 
    ON FRS.id_respuesta = R.id
    WHERE R.respuesta = '{tutor_id}'
      AND R.id_pregunta = 25
    ORDER BY FRS.fecha_hora_registro_respuesta DESC;
    """
    cur.execute(query.strip())
    rows = cur.fetchall()
    cur.close()
    
    # Extract the id_registro from each row
    id_registros = [id_registro[0] for id_registro in rows]
    
    return id_registros

print(get_tutors_ids())
#print(get_tutor_follow_ups('164266'))
# Fetch the dates
dates = get_student_follow_ups_dates('12560')

# Convert to DataFrame
df = pd.DataFrame(dates, columns=['Id ficha', 'Fecha ficha'])

# Export to Excel
df.to_excel('follow_up_dates.xlsx', index=False)

