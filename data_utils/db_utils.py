import psycopg2
from psycopg2 import sql


class DbUtils:
    def __init__(self, config):
        self.host = config['db_host']
        self.port = config['db_port']
        self.database = config['db_name']
        self.user = config['db_login']
        self.password = config['db_password']

    def create_table_if_not_exists(self, sql_file):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()

            with open(sql_file, 'r') as sql_file_contents:
                sql_statements = sql_file_contents.read()
                cursor.execute(sql_statements)

            conn.commit()
            conn.close()
            print(f"Table created successfully in {self.database}")
        except psycopg2.Error as e:
            print(f"PostgreSQL error: {e}")
