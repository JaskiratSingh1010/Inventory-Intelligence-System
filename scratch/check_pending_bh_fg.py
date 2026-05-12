import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    # Check for items with 0 stock but committed or on order
    cursor.execute("SELECT COUNT(*) FROM JIVO_OIL_HANADB.OITW WHERE \"WhsCode\"='BH-FG' AND \"OnHand\" = 0 AND (\"IsCommited\" > 0 OR \"OnOrder\" > 0)")
    print(f"Items with 0 OnHand but Committed/OnOrder in BH-FG: {cursor.fetchone()[0]}")
    c.close()
except Exception as e:
    print(e)
