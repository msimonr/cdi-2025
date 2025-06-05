import requests
import random
import time

API = "https://www.googleapis.com/books/v1/volumes?q=isbn:"
API_KEY = "AIzaSyBMTcWPXz0ZA7dzjcwjtfbo0x6YqhkhrQw"

def libro_existe(isbn):
    resp = requests.get(API+str(isbn)+"&key="+API_KEY)
    if resp.status_code != 200:
        print(f"⚠️ Error HTTP {resp.status_code} para ISBN {isbn}")
        return False, resp.status_code
    data = resp.json()
    return data.get("totalItems", 0) > 0, 200

#---
# MET3_ExactSemantica-Bool_ISBNDB
# Consulta una base de datos para verificar si el ISBN existe en la realidad.
#---

def met3ap1_exact_semantica_bool_isbndb(conn_db, conn_dbdq, table, id_row_col, isbn_column, id_exec):
    rows = []
    with conn_db.cursor() as cur:
        cur.execute(f'''
            SELECT {id_row_col}, {isbn_column} FROM "{table}"
        ''')
        rows = cur.fetchall()
        random.shuffle(rows)
        rows = rows[0:500]
        with conn_dbdq.cursor() as cur2:    
            for row in rows:
                id_tuple, isbn_tuple = row
                existe, status = libro_existe(isbn_tuple)
                cur2.execute(
                    'INSERT INTO "ResultCellRow" (target_table, id_exec, id_ap_method, row, attribute, dq_value) VALUES (%s, %s, %s, %s, %s, %s)',
                    (table, id_exec, 'MET3AP1_ExactSemantica-Bool_ISBNDB', id_tuple, isbn_column, int(existe))
                             )
                if status != 200:
                    print(f"🚨 Algo pasó con ISBN {isbn_tuple}: HTTP {status}")
                time.sleep(1)

def isbn_semantico_accuracy_ratio(conn_dqnl, table, attribute, id_exec):
    with conn_dqnl.cursor() as cur:
        cur.execute(f'''
                    SELECT
                    COUNT(*) FILTER (WHERE dq_value = 1)::float / COUNT(*) AS porcentaje_iguales_a_1
                    FROM public."ResultCellRow"
                    WHERE id_exec = {id_exec} AND target_table='{table}' AND id_ap_method = 'MET3AP1_ExactSemantica-Bool_ISBNDB';
                    ''')
        dq_value = cur.fetchone()[0]
        cur.execute('INSERT INTO "ResultColumn" (target_table, id_exec, id_ap_method, attribute, dq_value) VALUES (%s,%s, %s, %s, %s)',
                    (table, id_exec, "ISBNSemantico_AccuracyRatio", attribute, dq_value)
                    )