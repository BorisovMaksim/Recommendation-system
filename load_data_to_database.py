import pandas as pd


def drop_tables(conn):
    cur = conn.cursor()
    cur.execute("""DROP TABLE IF EXISTS playlist_track_map;""")
    cur.execute("""DROP TABLE IF EXISTS track;""")
    cur.execute("""DROP TABLE IF EXISTS playlist;""")
    conn.commit()


def csv_table_to_sql(engine, path, filename):
    df = pd.read_csv(path + filename)
    df.to_sql(filename[:-9], con=engine, if_exists='replace', index=False, chunksize=5)
