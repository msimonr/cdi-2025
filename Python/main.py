import psycopg as psycopg

from metrics.m1_exact_sintactica_bool import met1ap1_exact_sintactica_bool_isbn as met1ap1
from metrics.m1_exact_sintactica_bool import met2ap1_exact_sintactica_bool_fecha as met2ap1
from metrics.m1_exact_sintactica_bool import isbn_accuracy_ratio
from metrics.m2_exact_semantica_bool import met3ap1_exact_semantica_bool_isbndb as met3ap1
from metrics.m2_exact_semantica_bool import isbn_semantico_accuracy_ratio
from metrics.m3_comp_densidad_campos_identificadores_libros import met4ap1_comp_densidad_identificadores_libro as met4ap1
from metrics.m3_comp_densidad_campos_identificadores_libros import libros_densidad_promedio
from metrics.m4_comp_cobertura_top_100 import met5ap1_comp_cobertura_top100 as met5ap1
from metrics.m5_cons_intra_relacion_libros import met6ap1_cons_intra_relacion_libros as met6ap1
from metrics.m5_cons_intra_relacion_libros import cons_intra_relacion_libros_ratio_de_integridad
from metrics.m6_cons_interrelacion_isbnratings import met7ap1_cons_interrelacion_isbnratings as met7ap1

def commit(db):
    try:
        conn_dqnl.commit()
        print("Commit ejecutado")
    except Exception as e:
        conn_dqnl.rollback()
        print("Commit Fallido:", e)

def connect_to_db(dbname, host, port=5432, user="postgres", password='1234'):
    return psycopg.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password
    )
     
# Connect to the database
conn_nl = connect_to_db("NuevaLibreria", "localhost", 5432, "postgres", "1234")
conn_dqnl = connect_to_db("DQ_NuevaLibreria", "localhost", 5432, "postgres", "1234")

with conn_dqnl.cursor() as cur2:
    cur2.execute('INSERT INTO "Executions" DEFAULT VALUES RETURNING id_exec')
    id_exec = cur2.fetchone()[0]

print("METRICA: M1-ExactitudSintáctica-Bool")
print('Ejecutando: MET1AP1, Books')
met1ap1(conn_nl, conn_dqnl, "Books", "id_gen", "id", id_exec)
commit(conn_dqnl)
    
print('Ejecutando: MET1AP1, Ratings')
met1ap1(conn_nl, conn_dqnl, "Ratings", "id_gen", "id", id_exec)
commit(conn_dqnl)
    

print('Ejecutando: Agregacion, Books')
isbn_accuracy_ratio(conn_dqnl, "Books", "id", id_exec)
commit(conn_dqnl)
print('Ejecutando: Agregacion, Ratings')
isbn_accuracy_ratio(conn_dqnl, "Ratings", "id", id_exec)
commit(conn_dqnl)
print("-----")

print('Ejecutando: MET2AP1, Fecha')
met2ap1(conn_nl, conn_dqnl, "Books", "id_gen", "published_date", id_exec)
commit(conn_dqnl)
print("-----")

# print("METRICA: M2-ExactitudSemantica-Bool")
# print('Ejecutando: MET3AP1, Books')
# met3ap1(conn_nl, conn_dqnl, "Books", "id_gen", "id", id_exec)
# commit(conn_dqnl)
# print('Ejecutando: Agregacion, Books')
# isbn_semantico_accuracy_ratio(conn_dqnl, "Books", "id", id_exec)
# commit(conn_dqnl)
# print("-----")

print("METRICA: M3-CompDensidad_CamposIdentificadores")
print('Ejecutando: MET4AP1')
met4ap1(conn_nl, conn_dqnl, "Books", "id_gen", id_exec)
commit(conn_dqnl)
print('Ejecutando: Agregacion, Books')
libros_densidad_promedio(conn_dqnl, "Books", id_exec)
commit(conn_dqnl)
print("-----")

print("METRICA: M4-CompCobertura_Top100")
print('Ejecutando: MET5AP1')
met5ap1(conn_nl, conn_dqnl, "Books", "Authors", id_exec)
commit(conn_dqnl)
print("-----")

print("METRICA: M5-ConsIntraRelacion-Libros")
print('Ejecutando: MET6AP1')
met6ap1(conn_nl, conn_dqnl, id_exec)
commit(conn_dqnl)
print('Ejecutando: Agregacion, Books')
cons_intra_relacion_libros_ratio_de_integridad(conn_dqnl, id_exec)
commit(conn_dqnl)
print("-----")

print("METRICA: M6-ConsInterRelacion-ISBNRatings")
print('Ejecutando: MET7AP1')
met7ap1(conn_nl, conn_dqnl, "Ratings", "Books", id_exec)
commit(conn_dqnl)
print("-----")



print('Fin de ejecucion id_exec:', id_exec)
conn_nl.close()
conn_dqnl.close()

