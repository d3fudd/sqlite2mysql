import argparse
import sqlite3
import pymysql
from pymysql.constants import CLIENT
from tqdm import tqdm

BATCH_SIZE = 1000

def get_tables(sqlite_cursor):
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    return [row[0] for row in sqlite_cursor.fetchall()]

def get_table_schema(sqlite_cursor, table):
    sqlite_cursor.execute(f"PRAGMA table_info('{table}')")
    columns = []
    for cid, name, coltype, notnull, dflt_value, pk in sqlite_cursor.fetchall():
        coltype = coltype.upper() or 'TEXT'
        # Simplificação básica: mapeia tipos SQLite para MySQL
        if 'INT' in coltype:
            coltype_mysql = 'BIGINT'
        elif 'CHAR' in coltype or 'CLOB' in coltype or 'TEXT' in coltype:
            coltype_mysql = 'TEXT'
        elif 'BLOB' in coltype:
            coltype_mysql = 'LONGBLOB'
        elif 'REAL' in coltype or 'FLOA' in coltype or 'DOUB' in coltype:
            coltype_mysql = 'DOUBLE'
        elif 'NUM' in coltype or 'DEC' in coltype:
            coltype_mysql = 'DECIMAL(38,10)'
        else:
            coltype_mysql = 'TEXT'
        nullable = 'NOT NULL' if notnull else ''
        default = f"DEFAULT {dflt_value}" if dflt_value is not None else ''
        columns.append(f"`{name}` {coltype_mysql} {nullable} {default}")
    return columns

def create_table(mysql_cursor, table, columns):
    cols_sql = ', '.join(columns)
    sql = f"CREATE TABLE IF NOT EXISTS `{table}` ({cols_sql})"
    mysql_cursor.execute(sql)

def get_row_count(sqlite_cursor, table):
    sqlite_cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
    return sqlite_cursor.fetchone()[0]

def copy_table_data(sqlite_conn, mysql_conn, table):
    sqlite_cursor = sqlite_conn.cursor()
    mysql_cursor = mysql_conn.cursor()

    sqlite_cursor.execute(f"PRAGMA table_info('{table}')")
    col_names = [row[1] for row in sqlite_cursor.fetchall()]
    placeholders = ', '.join(['%s'] * len(col_names))
    columns_str = ', '.join(f"`{c}`" for c in col_names)

    total_rows = get_row_count(sqlite_cursor, table)
    tqdm_bar = tqdm(total=total_rows, desc=f"Copiando {table}", unit="linhas")

    offset = 0
    while True:
        sqlite_cursor.execute(f"SELECT * FROM `{table}` LIMIT ? OFFSET ?", (BATCH_SIZE, offset))
        rows = sqlite_cursor.fetchall()
        if not rows:
            break
        sql = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"
        try:
            mysql_cursor.executemany(sql, rows)
            mysql_conn.commit()
        except Exception as e:
            print(f"Erro ao inserir no MySQL: {e}")
            mysql_conn.rollback()
        tqdm_bar.update(len(rows))
        offset += BATCH_SIZE
    tqdm_bar.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sqlite', required=True)
    parser.add_argument('--mysql', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--database', required=True)
    args = parser.parse_args()

    sqlite_conn = sqlite3.connect(args.sqlite)
    mysql_conn = pymysql.connect(host=args.mysql, user=args.user, password=args.password, database=args.database, client_flag=CLIENT.MULTI_STATEMENTS)

    sqlite_cursor = sqlite_conn.cursor()
    mysql_cursor = mysql_conn.cursor()

    tables = get_tables(sqlite_cursor)
    print(f"Tabelas encontradas: {tables}")

    for table in tables:
        print(f"Criando tabela {table} no MySQL...")
        columns = get_table_schema(sqlite_cursor, table)
        create_table(mysql_cursor, table, columns)

    for table in tables:
        print(f"Iniciando cópia da tabela {table}...")
        copy_table_data(sqlite_conn, mysql_conn, table)

    sqlite_conn.close()
    mysql_conn.close()

if __name__ == "__main__":
    main()
