import psycopg as psycopg
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

def connect_to_db(dbname, host, port=5432, user="postgres", password='1234'):
    return psycopg.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password
    )

ID_EJECUCION_SEMANTICA = 57 #Fija porque cuesta $$ correrla

conn_dqnl = connect_to_db("DQ_NuevaLibreria", "localhost", 5432, "postgres", "1234")

#Obtener ultimo id de ejecucion
with conn_dqnl.cursor() as cur:
    cur.execute('''
            SELECT * FROM "Executions"
            ORDER BY id_exec DESC
    '''
    )
    id_ejecucion = cur.fetchone()


# # Obtener datos de metodos aplicados ISBN SINTACTICO
# with conn_dqnl.cursor() as cur:
#     cur.execute('''
#             SELECT "row" as id_gen, dq_value FROM "ResultCellRow"
#             WHERE target_table='Books' AND id_ap_method = 'MET1AP1_ExactSintáctica-Bool_ISBN' AND id_exec = %s
#             ORDER BY id_gen ASC
#     ''',(id_ejecucion[0],))
#     rows = cur.fetchall()
    
# diccionario = {row[0]: row[1] for row in rows}
    
    
# ISBN Accuracy
# with conn_dqnl.cursor() as cur: 
#     cur.execute('''
#             SELECT target_table, dq_value FROM "ResultColumn"
#             WHERE id_ap_method = 'ISBN_AccuracyRatio' AND id_exec = %s
#     ''',(id_ejecucion[0],))
#     rows = cur.fetchall()


# # ISBN Semantico
# rows_semantico = []
# with conn_dqnl.cursor() as cur:
#     cur.execute('''
#             SELECT "row" as id_gen, dq_value FROM "ResultCellRow"
#             WHERE target_table='Books' AND id_ap_method = 'MET3AP1_ExactSemantica-Bool_ISBNDB' AND id_exec = %s
#             ORDER BY id_gen ASC
#     ''',(ID_EJECUCION_SEMANTICA,))
#     rows_semantico = cur.fetchall()
    
    
# # ISBNSemantico_AccuracyRatio
# with conn_dqnl.cursor() as cur: 
#     cur.execute('''
#             SELECT dq_value FROM "ResultColumn"
#             WHERE id_ap_method = 'ISBNSemantico_AccuracyRatio' AND id_exec = %s
#     ''',(ID_EJECUCION_SEMANTICA,))
#     value_semantico = cur.fetchone()[0]


# x_axis = [row[0] for row in rows_semantico]
# y_axis = [row[1] for row in rows_semantico]

# y_axis = [0,0]
# for row in rows_semantico:
#     if(row[1] == 0):
#         y_axis[0] += 1
#     else:
#         y_axis[1] += 1

# cantidad_semantico_incorrecto = 0
# for row in rows_semantico:
#     if(row[1] == 0):
#         cantidad_semantico_incorrecto += diccionario[row[0]]
# cantidad_semantico_incorrecto = (cantidad_semantico_incorrecto*100)/500
    

# Obtener datos de metodos aplicados M3

# with conn_dqnl.cursor() as cur:
#     cur.execute('''
#             SELECT "row" as id_gen, dq_value FROM "ResultCellRow"
#             WHERE target_table='Books' AND id_ap_method = 'MET4AP1CompDensidad_CamposIdentificadores_Libro' AND id_exec = %s
#             ORDER BY id_gen ASC
#     ''',(id_ejecucion[0],))
#     rows = cur.fetchall()

# with conn_dqnl.cursor() as cur:
#     cur.execute('''
#             SELECT dq_value FROM "ResultTable"
#             WHERE target_table='Books' AND id_ap_method = 'LibrosDensidad_Promedio' AND id_exec = %s
#     ''',(id_ejecucion[0],))
#     agregacion = cur.fetchone()[0]


# densidad = [row[1] for row in rows]

# conteo = {}
# for valor in densidad:
#     conteo[valor] = conteo.get(valor, 0) + 1

with conn_dqnl.cursor() as cur:
    cur.execute('''
            SELECT "row" as id_gen, dq_value FROM "ResultCellRow"
            WHERE target_table='Books' AND id_ap_method = 'MET6AP1ConsIntraRelacion-Libros' AND id_exec = %s
            ORDER BY id_gen ASC
    ''',(id_ejecucion[0],))
    rows = cur.fetchall()

with conn_dqnl.cursor() as cur:
    cur.execute('''
            SELECT dq_value FROM "ResultTable"
            WHERE target_table='Books' AND id_ap_method = 'ConsIntraRelacion-Libros_RatioDeIntegridad' AND id_exec = %s
    ''',(id_ejecucion[0],))
    dq_value_promedio = cur.fetchone()[0]

y_axis = [0,0]
for row in rows:
    if(row[1] == 0):
        y_axis[0] += 1
    else:
        y_axis[1] += 1

plt.title("MET6AP1ConsIntraRelacion-Libros", fontsize=10)
plt.xlabel("DQ Value (Boolean)")
plt.ylabel("Cantidad de tuplas")

plt.bar([0,1], y_axis, width=0.2, label='Tuplas en la tabla Books')
plt.text(0, y_axis[0], y_axis[0], fontweight="bold", ha='center', va='bottom')
plt.text(1, y_axis[1], y_axis[1], fontweight="bold", ha='center', va='bottom')
plt.axhline(dq_value_promedio*(len(rows)), color='red', linestyle='--', linewidth=2, label=f"Ratio de integridad = {dq_value_promedio:.2f}%")


# plt.ylim(0, 100)
plt.legend()
plt.tight_layout()
plt.show()

conn_dqnl.close()