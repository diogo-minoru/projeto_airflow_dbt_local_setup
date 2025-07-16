from sqlalchemy import create_engine, text

# SQL Server
sqlserver_engine = create_engine(
    "mssql+pyodbc://admin:admin@localhost:1433/Contoso?"
    "driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    "&authentication=ActiveDirectoryIntegrated"
)

# PostgreSQL
pg_engine = create_engine("postgresql+psycopg2://dbtuser:dbtpassword@localhost:5433/dbt_db")

print("=== Testando SQL Server ===")
try:
    with sqlserver_engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS test"))
        print("Conexão com SQL Server bem-sucedida!")
        for row in result:
            print(row)
except Exception as e:
    print("Erro SQL Server:", e)

print("\n=== Testando PostgreSQL ===")
try:
    with pg_engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        print("Conexão com PostgreSQL bem-sucedida!")
        for row in result:
            print(row)
except Exception as e:
    print("Erro PostgreSQL:", e)
