from src.migrate_sqlserver import SqlServerToPostgresMigrator
from sqlalchemy import create_engine

# SQL Server
sqlserver_engine = create_engine(
    "mssql+pyodbc://admin:admin@localhost:1433/Contoso?"
    "driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    "&authentication=ActiveDirectoryIntegrated"
)

# PostgreSQL
pg_engine = create_engine("postgresql+psycopg2://dbtuser:dbtpassword@localhost:5433/dbt_db")

# Nome da tabela destino no Postgres
tabela_pg = 'customer'

# Consulta SQL da tabela que será migrada
consulta_sql = """
SELECT customerkey,
       givenname,
       surname,
       city,
       statefull,
       countryfull,
       age
FROM data.customer
"""

def migrar_tabela():
    migrator = SqlServerToPostgresMigrator(sqlserver_engine, pg_engine)
    migrator.migrate(consulta_sql=consulta_sql, tabela_pg=tabela_pg)

if __name__ == "__main__":
    migrar_tabela()
