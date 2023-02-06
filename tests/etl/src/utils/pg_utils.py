def clean_pg(pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute("TRUNCATE content.person_film_work CASCADE;")
        pg_conn.commit()
        cursor.execute("TRUNCATE content.genre_film_work CASCADE;")
        pg_conn.commit()
        cursor.execute("TRUNCATE content.genre CASCADE;")
        pg_conn.commit()
        cursor.execute("TRUNCATE content.person CASCADE;")
        pg_conn.commit()
        cursor.execute("TRUNCATE content.film_work CASCADE;")
        pg_conn.commit()
