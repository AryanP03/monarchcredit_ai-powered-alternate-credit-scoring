from immudb.client import ImmudbClient

client = ImmudbClient()

# login (default credentials unless you changed)
client.login("immudb", "immudb")

# switch to your DB
client.useDatabase("credit_audit_db")

# get all tables
tables = client.sqlQuery("SELECT name FROM tables();")

print("\n===== DATABASE SCHEMA =====\n")

for table in tables:
    table_name = table[0]
    print(f"\n📦 Table: {table_name}")

    columns = client.sqlQuery(f"SELECT * FROM columns('{table_name}');")

    print("Column Name | Type | Nullable | Indexed")
    print("-" * 50)

    for col in columns:
        print(f"{col[0]} | {col[1]} | {col[2]} | {col[3]}")