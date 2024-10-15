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

# Diccionaros para mapear los campos de las dimensiones a las posiciones en la fila
dicts_individual = [
    {
        "key": "proyecto_vida",
        "index": 11
    },
    {
        "key": "rasgos_personalidad",
        "index": 8
    },
    {
        "key": "red_apoyo",
        "index": 10
    },
    {
        "key": "aspectos_motivacionales",
        "index": 13
    },
    {
        "key": "relacion_erotico_afectiva",
        "index": 15
    },
    {
        "key": "autoconocimiento",
        "index": 7
    },
    {
        "key": "identificacion",
        "index": 9
    },
    {
        "key": "diversidad_sexual",
        "index": 16
    },
    {
        "key": "salud",
        "index": 12
    },
    {
        "key": "historia_vida",
        "index": 14
    }
]

dicts_academico = [
    {
        "key": "desemp_acad",
        "index": 22
    },
    {
        "key": "manej_tiempo",
        "index": 24
    },
    {
        "key": "elect_vocacional",
        "index": 23
    }
]

dicts_familiar = [
    {
        "key": "dinamica_familiar",
        "index": 19
    }
]

dicts_economico = [
    {
        "key": "apoyos_institucionales",
        "index": 27
    },
    {
        "key": "finanzas",
        "index": 28
    },
    {
        "key": "apoyo_econo_familiar",
        "index": 29
    },
    {
        "key": "situa_laboral_ocupa",
        "index": 30
    },
]

dicts_vida_U = [
    {
        "key": "motivación_acomp",
        "index": 33
    },
    {
        "key": "refer_geografica",
        "index": 34
    },
    {
        "key": "adapt_ciuda_universidad",
        "index": 35
    },
    {
        "key": "ofert_servicios_vida_univer",
        "index": 36
    },
    {
        "key": "vivienda",
        "index": 37
    },
    {
        "key": "vincu_grupos",
        "index": 38
    }
]

def set_dimension_themes(array, search_params, dict_list):
    for search_param in search_params:
        for dictionary in dict_list:
            if dictionary['key'] == search_param:
                index = dictionary['index']
                if 0 <= index < len(array):
                    array[index] = True


#function to get all the ids from the tutors

def get_tutors_ids():
    cur = con.cursor()
    cur.execute("SELECT DISTINCT respuesta FROM mdl_talentospilos_df_respuestas mtdr WHERE id_pregunta = 61 AND respuesta != ''")
    rows = cur.fetchall()
    cur.close()
    formatted_rows = [respuesta[0] for respuesta in rows]
    
    return formatted_rows

def get_tutors_info():
    tutor_ids = get_tutors_ids()
    
    # Convert list of IDs to a comma-separated string for the SQL query
    tutor_ids_str = ','.join([str(tutor_id) for tutor_id in tutor_ids])
    
    cur = con.cursor()
    query = f"""
    SELECT substring(mu.username,0,8) as username, firstname, lastname, email
    FROM mdl_user mu
    WHERE id IN ({tutor_ids_str})
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    
    return rows

# Example usage
tutors_info = get_tutors_info()

""" for tutor in tutors_info:
    print(f"Code:{tutor[0]} Firstname: {tutor[1]}, Lastname: {tutor[2]}, Email: {tutor[3]}") """
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
      AND R.id_pregunta = 61
    ORDER BY FRS.fecha_hora_registro_respuesta DESC;
    """
    cur.execute(query.strip())
    rows = cur.fetchall()
    cur.close()
    
    # Extract the id_registro from each row
    id_registros = [id_registro[0] for id_registro in rows]
    
    return id_registros

def get_follow_up_data(id): 
    cur = con.cursor()
    query = f"""
    SELECT id_pregunta, enunciado, respuesta FROM mdl_talentospilos_df_preguntas P 
                INNER JOIN (
                SELECT * FROM (
                    SELECT id AS id_form_preg, id_pregunta AS id_tabla_preguntas FROM mdl_talentospilos_df_form_preg 
                    ) FP INNER JOIN (SELECT * 
                                FROM mdl_talentospilos_df_respuestas AS R 
                                INNER JOIN 
                                    (
                                        SELECT * 
                                        FROM mdl_talentospilos_df_form_resp AS FR 
                                        INNER JOIN mdl_talentospilos_df_form_solu AS FS 
                                        ON FR.id = FS.id_formulario_respuestas 
                                        WHERE FR.id = '{id}'
                                    ) AS FRS 
                                ON FRS.id_respuesta = R.id) RF
                            ON RF.id_pregunta = FP.id_form_preg) TT
                ON id_tabla_preguntas = P.id;
    """
    cur.execute(query.strip())
    rows = cur.fetchall()
    cur.close()
    return rows

print(get_tutor_follow_ups('164266'))
print(get_follow_up_data('217357'))
# Fetch the dates
dates = get_student_follow_ups_dates('12560')

# Convert to DataFrame
df = pd.DataFrame(dates, columns=['Id ficha', 'Fecha ficha'])

# Export to Excel
df.to_excel('follow_up_dates.xlsx', index=False)

def save_tutors_info_to_excel():
    tutors_info = get_tutors_info()
    
    # Convert to DataFrame
    df = pd.DataFrame(tutors_info, columns=['Username', 'Firstname', 'Lastname', 'Email'])
    
    # Export to Excel
    df.to_excel('tutors_info.xlsx', index=False)
    print("Data has been written to tutors_info.xlsx")

def translate_follow_up(datos_list, file_path):
    columnas = ['fecha', 'lugar', 'hora_inicio', 'hora_fin', 'objetivos', 'individual', 'riesgos_individual', 'autoconocimiento','rasgos_de_personalidad', 'identificacion', 'red_de_apoyo', 'proyecto_de_vida', 'salud', 'aspectos_motivacionales', 'historia_de_vida', 'relacion_erotico_afectivas', 'diversidad_sexual', 'familiar', 'riesgo_familiar', 'dinamica_familiar', 'academico', 'riesgo_academico', 'desempeño_academico', 'eleccion_vocacional', 'manejo_del_tiempo', 'economico', 'riesgo_economico', 'apoyos_economicos', 'manejo_finanzas', 'apoyo_economico', 'situacion_laboral_ocupacional', 'vida_universitaria_ciudad', 'riesgo_vida_universitaria_ciudad', 'motivación_compañamiento', 'referencia_geográfica', 'adaptación_ciudad_Universidad', 'oferta_servicios', 'vivienda', 'vinculación_grupos_actividades_extracurriculares', 'apoyo_académico', 'taller_par_par', 'reconocimiento_ciudad_U', 'rem_profesional_SE', 'rem_racticante_SE', 'rem_actividades_grupales', 'rem_monitorías_académicas', 'rem_proyectos_Universidad', 'rem_servicio_salud', 'rem_registro_académico', 'rem_matrícula_financiera', 'rem_desarrollo_humano_promoción_SE', 'rem_directores_programa', 'rem_grupos_universidad', 'rem_externa', 'Ninguna_acción_realizada', 'observaciones', 'revisado_profesional', 'revisado_practicante', 'primer_acercamiento', 'cierre', 'id_estudiante', 'id_creador', 'id_modificador']
    
    # Initialize an empty list to collect all rows
    all_rows = []

    # Loop over each form data in the array
    for datos in datos_list:
        filas = [None] * 63
        
        # Set default values for different dimensions
        filas[7:17] = [False] * 10  # Individual
        filas[19] = False  # Familiar
        filas[22:25] = [False] * 3  # Academic
        filas[27:31] = [False] * 4  # Economic
        filas[33:39] = [False] * 6  # University Life
        filas[39:55] = [False] * 16  # New fields
        filas[56] = True  # Professional reviewed
        filas[57] = True  # Intern reviewed
        filas[58] = False  # First approach
        filas[59] = False  # Closure
        filas[62] = 1  # Default modifier ID
        
        #Parametros dimension individual
        ind_params = list(filter(lambda tripla: tripla[0] == 10, datos))
        # Setear las tematicas de la dimension individual
        set_dimension_themes(filas, ind_params[0], dicts_individual)

        #Parametros dimension familiar
        fam_params = list(filter(lambda tripla: tripla[0] == 11, datos))
        # Setear las tematicas de la dimension familiar
        set_dimension_themes(filas, fam_params[0], dicts_familiar)

        #Parametros dimension economica
        eco_params = list(filter(lambda tripla: tripla[0] == 12, datos))
        # Setear las tematicas de la dimension economica
        set_dimension_themes(filas, eco_params[0], dicts_economico)

        #Parametros dimension academica
        acad_params = list(filter(lambda tripla: tripla[0] == 13, datos))
        # Setear las tematicas de la dimension academica
        set_dimension_themes(filas, acad_params[0], dicts_academico)

        #Parametros dimension vida universitaria
        vida_U_params = list(filter(lambda tripla: tripla[0] == 14, datos))
        # Setear las tematicas de la dimension vida universitaria
        set_dimension_themes(filas, vida_U_params[0], dicts_vida_U)
        # Mapping ID to index
        id_a_indice = {
            1: 0,
            2: 1,
            3: 2,
            4: 3,
            6: 4,
            7: 5,
            9: 6,
            8: 17,
            15: 18,
            16: 20,
            18: 21,
            17: 25,
            19: 26,
            20: 31,
            21: 32,
            26: 55,
            25: 60,
            24: 61,
        }
        
        # Process each form field
        for dato in datos:
            id, key, value = dato
            if id in id_a_indice:
                indice = id_a_indice[id]
                # Verificar si el id necesita alguna conversión específica
                if id == 1:
                    date_value = datetime.strptime(str(value), '%Y-%m-%d')
                    filas[indice] = date_value.strftime('%Y-%m-%d')  # Convertir a formato de fecha YYYY-MM-DD
                elif id == 3 or id == 4:
                    time_value = datetime.strptime(str(value), '%H:%M')
                    filas[indice] = time_value.strftime('%H:%M')  # Convertir a formato de hora HH:MM
                elif id == 25 or id == 24:
                    filas[indice] = int(value)  # Convertir a entero
                elif id == 6 or id == 18 or id == 21 or id == 26 or id == 32:
                    if(value == '-#$%-'):
                        filas[indice] = ''
                else:
                    filas[indice] = value  # Asignación directa
        # Append the processed row to the list
        all_rows.append(filas)
    
    # Convert all rows to a DataFrame
    df = pd.DataFrame(all_rows, columns=columnas)

    # Append the DataFrame to the CSV file (create or append mode)
    df.to_csv(file_path, mode='a', header=not pd.io.common.file_exists(file_path), index=False)
    print(f'Datos guardados en {file_path}')

def store_follow_ups_with_pagination(limit, offset, file_path):
    # Get the tutor IDs (or you can adjust this to get any specific IDs you need)
    tutor_ids = get_tutors_ids()
    
    # Apply the offset and limit to paginate the tutor IDs
    paginated_tutor_ids = tutor_ids[offset:offset + limit]

    # Initialize an empty list to collect all follow-up data
    all_follow_up_data = []

    # For each tutor ID in the paginated list, get their follow-up data
    for tutor_id in paginated_tutor_ids:
        follow_up_ids = get_tutor_follow_ups(tutor_id)
        
        # For each follow-up ID, retrieve the data and append to the list
        for follow_up_id in follow_up_ids:
            follow_up_data = get_follow_up_data(follow_up_id)
            all_follow_up_data.append(follow_up_data)
    
    # Store all follow-up data in the CSV using the translate_follow_up function
    translate_follow_up(all_follow_up_data, file_path)

    print(f'Stored {len(all_follow_up_data)} follow-ups to {file_path}')

# Example usage
store_follow_ups_with_pagination(limit=10, offset=0, file_path='follow_up_data_paginated.csv')
save_tutors_info_to_excel()