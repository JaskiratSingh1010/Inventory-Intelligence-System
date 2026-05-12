import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    # List all schemas to see if there are other companies
    cursor.execute("SELECT SCHEMA_NAME FROM SCHEMAS")
    schemas = [r[0] for r in cursor.fetchall() if 'JIVO' in r[0]]
    print(f"JIVO Schemas: {schemas}")
    
    for s in schemas:
        try:
            cursor.execute(f"SELECT COUNT(*), SUM(\"OnHand\"), SUM(\"StockValue\") FROM {s}.OITW WHERE \"WhsCode\"='BH-FG' AND \"OnHand\" != 0")
            row = cursor.fetchone()
            if row[0] > 0:
                print(f"Schema {s}: {row[0]} items, Qty: {row[1]}, Val: {row[2]}")
        except: pass
    c.close()
except Exception as e:
    print(e)
