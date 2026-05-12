import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    cursor.execute("SELECT \"WhsCode\", COUNT(*) FROM JIVO_OIL_HANADB.OITW WHERE \"OnHand\">0 GROUP BY \"WhsCode\" ORDER BY 2 DESC")
    rows = cursor.fetchall()
    for r in rows:
        print(f"{r[0]}: {r[1]}")
    c.close()
except Exception as e:
    print(e)
