from datetime import datetime
#---
# MET1_ExactSintáctica-Bool_ISBN
# Verifica si el string de entrada corresponde con el formato ISBN-10 o ISBN-13.
#---

def isbn_valid(isbn):
    suma = 0
    try:
        if len(isbn) == 10:
            for i in range(9):
                suma += int(isbn[i]) * (i+1)
            suma = suma % 11
            if (isbn[9] == 'X'):
                return suma == 10
            else:
                return suma == int(isbn[9])                   
        elif len(isbn) == 13:
            for i in range(12):
                if(i % 2 == 0):
                    suma += int(isbn[i])
                else:
                    suma += int(isbn[i]) * 3
            return ((suma + int(isbn[12])) % 10 == 0)
    except:
        #print("Cast error")
        return False 
    #print("Cantidad de digitos equivodada")
    return False

def met1ap1_exact_sintactica_bool_isbn(conn_db, conn_dbdq, table, id_row_col, isbn_column, id_exec):
    rows = []
    with conn_db.cursor() as cur:
        cur.execute(f'''
            SELECT {id_row_col}, {isbn_column} FROM "{table}"
        ''')
        rows = cur.fetchall()
        with conn_dbdq.cursor() as cur2:    
            for row in rows:
                id_tuple, isbn_tuple = row
                cur2.execute(
                    'INSERT INTO "ResultCellRow" (target_table, id_exec, id_ap_method, row, attribute, dq_value) VALUES (%s, %s, %s, %s, %s, %s)',
                    (table, id_exec, 'MET1AP1_ExactSintáctica-Bool_ISBN', id_tuple, isbn_column, int(isbn_valid(isbn_tuple)))
                             )

#---
# MET2_ExactSintáctica-Bool_Fecha
# Verifica si el string de entrada corresponde con el formato ISBN-10 o ISBN-13.
#---

def date_valid(date):
    # AAAA
    # AAAA-MM
    # AAAA-MM-DD
    try:
        if(len(date) == 4):
            #Solo hay año
            return int(date) > 0
        elif(len(date) == 7):
            #Hay año y mes
            datetime.strptime(date, "%Y-%m")
            return True
        elif(len(date) == 10):
            datetime.strptime(date, "%Y-%m-%d")
            return True            
    except:
        return False
    return False
    

def met2ap1_exact_sintactica_bool_fecha(conn_db, conn_dbdq, table, id_row_col, date_column, id_exec):
    rows = []
    with conn_db.cursor() as cur:
        cur.execute(f'''
            SELECT {id_row_col}, {date_column} FROM "{table}"
        ''')
        rows = cur.fetchall()
        with conn_dbdq.cursor() as cur2:
            for row in rows:
                id_tuple, date_tuple = row
                cur2.execute(
                    'INSERT INTO "ResultCellRow" (target_table, id_exec, id_ap_method, row, attribute, dq_value) VALUES (%s, %s, %s, %s, %s, %s)',
                    (table, id_exec, 'MET2AP1_ExactSintáctica-Bool_Fecha', id_tuple, date_column, int(date_valid(date_tuple)))
                             )


def isbn_accuracy_ratio(conn_dqnl, table, attribute, id_exec):
    with conn_dqnl.cursor() as cur:
        cur.execute('''
                    SELECT
                    COUNT(*) FILTER (WHERE dq_value = 1)::float / NULLIF(COUNT(*), 0)
                    FROM public."ResultCellRow"
                    WHERE id_exec = %s AND target_table=%s AND id_ap_method = 'MET1AP1_ExactSintáctica-Bool_ISBN';
                    ''', (id_exec, table))
        dq_value = cur.fetchone()[0] or 0.0
        cur.execute('INSERT INTO "ResultColumn" (target_table, id_exec, id_ap_method, attribute, dq_value) VALUES (%s,%s, %s, %s, %s)',
                    (table, id_exec, "ISBN_AccuracyRatio", attribute, dq_value)
                    )