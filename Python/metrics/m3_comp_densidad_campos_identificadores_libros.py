# ---
# MET4AP1CompDensidad_CamposIdentificadores_Libro
# Mide que porcentaje de campos clave (ISBN, Title, Author, Publisher, Publish_Date) no son nulos.
# ---

def met4ap1_comp_densidad_identificadores_libro(conn_db, conn_dbdq, table, id_row_col, id_exec):
    atributos = ["id", "title", "author_id", "publisher", "published_date"]
    with conn_db.cursor() as cur:
        cur.execute(f'''
            SELECT {id_row_col}, {", ".join(atributos)}
            FROM "{table}"
        ''')
        rows = cur.fetchall()

        with conn_dbdq.cursor() as cur2:
            for row in rows:
                id_tuple = row[0]
                valores = row[1:]
                total_attrs = len(valores)
                no_nulos = sum(1 for val in valores if val not in [None, '', 'null', 'NULL', 99362.0])
                densidad = no_nulos / total_attrs
                cur2.execute(
                    '''INSERT INTO "ResultCellRow" 
                    (target_table, id_exec, id_ap_method, row, dq_value)
                    VALUES (%s, %s, %s, %s, %s)''',
                    (table, id_exec, 'MET4AP1CompDensidad_CamposIdentificadores_Libro', id_tuple, densidad)
                )
                
def libros_densidad_promedio(conn_dqnl, table, id_exec):
    with conn_dqnl.cursor() as cur:
        cur.execute('''
                    SELECT dq_value
                    FROM public."ResultCellRow"
                    WHERE id_exec = %s AND target_table= %s AND id_ap_method = 'MET4AP1CompDensidad_CamposIdentificadores_Libro';
                    ''', (id_exec, table))
        rows = cur.fetchall()
        dq_value = 0
        if rows:
           dq_value = sum(row[0] for row in rows) / len(rows) 
        cur.execute('INSERT INTO "ResultTable" (target_table, id_exec, id_ap_method, dq_value) VALUES (%s,%s, %s, %s)',
                    (table, id_exec, "LibrosDensidad_Promedio", dq_value)
                    )