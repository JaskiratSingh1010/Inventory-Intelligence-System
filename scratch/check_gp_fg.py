import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    cursor.execute("SELECT COUNT(*) FROM JIVO_OIL_HANADB.OITW WHERE \"WhsCode\"='GP-FG' AND \"OnHand\" > 0")
    print(f"Items in GP-FG: {cursor.fetchone()[0]}")
    c.close()
except Exception as e:
    print(e)
