# ---
# MET7AP1ConsInterRelacion-ISBNRatings
# Calcula el porcentaje de ISBN unicos en Ratings que tambien existen en Libros.
# ---

def met7ap1_cons_interrelacion_isbnratings(conn_db, conn_dbdq, table_ratings, table_libros, id_exec):
    with conn_db.cursor() as cur:

        cur.execute(f'''
            SELECT DISTINCT id FROM "{table_ratings}" 
            WHERE id IS NOT NULL AND id <> ''
        ''')
        isbns_ratings = set(row[0] for row in cur.fetchall())

        if not isbns_ratings:
            dq_value = 0.0  
        else:

            cur.execute(f'''
                SELECT DISTINCT id FROM "{table_libros}"
                WHERE id IS NOT NULL AND id <> ''
            ''')
            isbns_libros = set(row[0] for row in cur.fetchall())

            encontrados = len(isbns_ratings.intersection(isbns_libros))
            total = len(isbns_ratings)
            dq_value = encontrados / total

    with conn_dbdq.cursor() as cur2:
        cur2.execute(
            '''INSERT INTO "ResultColumn" 
            (target_table, id_exec, id_ap_method, attribute ,dq_value)
            VALUES (%s, %s, %s, %s, %s)''',
            (table_ratings, id_exec, 'MET7AP1ConsInterRelacion-ISBNRatings', 'id' , dq_value)
        )
