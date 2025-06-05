# ---
# MET6AP1ConsIntraRelacion-Libros
# Verifica si la dependencia funcional (Title, Author_id, Publisher, Publish_Date) → ISBN se cumple.
# ---

def met6ap1_cons_intra_relacion_libros(conn_db, conn_dbdq, id_exec):
    with conn_db.cursor() as cur:
        cur.execute(f'''
                WITH conteos AS (
                    SELECT
                        COALESCE(title, '') AS title,
                        COALESCE(author_id, -1) AS author_id,
                        COALESCE(publisher, '') AS publisher,
                        COALESCE(published_date, '') AS published_date,
                        COUNT(DISTINCT id) AS isbn_count
                FROM "Books"
                GROUP BY 1, 2, 3, 4
                )
                SELECT
                    b.id_gen, b.id, b.title, b.author_id, b.publisher, b.published_date,
                    CASE WHEN c.isbn_count = 1 THEN 1 ELSE 0 END AS dq_value
                FROM "Books" b
                JOIN conteos c
                    ON COALESCE(b.title, '') = c.title
                AND COALESCE(b.author_id, -1) = c.author_id
                AND COALESCE(b.publisher, '') = c.publisher
                AND COALESCE(b.published_date, '') = c.published_date;
        ''')
        rows = cur.fetchall()

        with conn_dbdq.cursor() as cur2:
            for row in rows:
                id_gen, _, _, _, _, _, dq_value = row
                cur2.execute('''
                    INSERT INTO "ResultCellRow" 
                    (target_table, id_exec, id_ap_method, row, dq_value)
                    VALUES (%s, %s, %s, %s, %s)
                ''', ("Books",id_exec,'MET6AP1ConsIntraRelacion-Libros', id_gen, dq_value)
                )

def cons_intra_relacion_libros_ratio_de_integridad(conn_dqnl, id_exec):
    with conn_dqnl.cursor() as cur:
        cur.execute('''
                    SELECT
                    COUNT(*) FILTER (WHERE dq_value = 1)::float / NULLIF(COUNT(*), 0)
                    FROM public."ResultCellRow"                    
                    WHERE id_exec = %s AND target_table='Books' AND id_ap_method = 'MET6AP1ConsIntraRelacion-Libros';
                    ''', (id_exec,))
        dq_value = cur.fetchone()[0] or 0.0
        cur.execute('INSERT INTO "ResultTable" (target_table, id_exec, id_ap_method, dq_value) VALUES (%s,%s, %s, %s)',
                    ('Books', id_exec, "ConsIntraRelacion-Libros_RatioDeIntegridad", dq_value)
                    )