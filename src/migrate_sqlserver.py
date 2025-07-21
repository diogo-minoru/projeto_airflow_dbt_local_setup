from sqlalchemy import Table, MetaData, Column, inspect, text
from sqlalchemy import String, Float, TIMESTAMP, BigInteger, Integer, Numeric
import time

class SqlServerToPostgresMigrator:
    def __init__(self, sqlserver_engine, pg_engine, schema_pg: str = 'public'):
        self.sqlserver_engine = sqlserver_engine
        self.pg_engine = pg_engine
        self.schema_pg = schema_pg

    def _get_column_type(self, sqlserver_type):
        sql_type = str(sqlserver_type).lower()
        if 'int' in sql_type:
            return Integer
        elif 'bigint' in sql_type:
            return BigInteger
        elif 'decimal' in sql_type or 'numeric' in sql_type:
            return Numeric
        elif 'float' in sql_type or 'real' in sql_type:
            return Float
        elif 'char' in sql_type or 'text' in sql_type or 'varchar' in sql_type or 'nchar' in sql_type or 'nvarchar' in sql_type:
            return String
        elif 'date' in sql_type or 'time' in sql_type or 'datetime' in sql_type:
            return TIMESTAMP
        else:
            return String

    def migrate(self, consulta_sql: str, tabela_pg: str):
        print(f"=== Iniciando migração da tabela '{tabela_pg}' ===")

        # Conexão SQL Server via SQLAlchemy
        with self.sqlserver_engine.connect() as sql_conn:
            start_query = time.time()
            result = sql_conn.execute(text(consulta_sql))
            columns = result.keys()
            rows = result.fetchall()
            print(f"[{tabela_pg}] Consulta executada em {time.time() - start_query:.2f}s.")

        # Se não tiver dados, sai
        if not rows:
            print(f"[{tabela_pg}] Nenhum dado encontrado.")
            return

        # Descobre tipos das colunas com base no primeiro registro
        sample_row = rows[0]
        pg_columns = []
        for col, value in zip(columns, sample_row):
            if value is None:
                pg_columns.append(Column(col.lower(), String))  # default se None
            else:
                pg_columns.append(Column(col.lower(), self._get_column_type(type(value))))

        # Define tabela no Postgres
        metadata = MetaData(schema=self.schema_pg)
        pg_table = Table(tabela_pg, metadata, *pg_columns)

        # Conexão PostgreSQL (já com engine passada no init)
        connection_pg = self.pg_engine.connect()
        inspector = inspect(self.pg_engine)

        # Drop se já existe
        if inspector.has_table(tabela_pg, schema=self.schema_pg):
            pg_table.drop(self.pg_engine)

        metadata.create_all(self.pg_engine)

        # Inserção
        rows_dict = [dict(zip(columns, row)) for row in rows]
        print(f"[{tabela_pg}] Inserindo {len(rows_dict)} registros...")

        start_insert = time.time()
        with connection_pg.begin():
            try:
                connection_pg.execute(pg_table.insert(), rows_dict)
                print(f"[{tabela_pg}] Inserção concluída em {time.time() - start_insert:.2f}s.")
            except Exception as e:
                print(f"[{tabela_pg}] Erro ao inserir dados: {e}")
                connection_pg.rollback()

        connection_pg.close()
        print(f"=== Migração concluída para '{tabela_pg}' ===\n")
