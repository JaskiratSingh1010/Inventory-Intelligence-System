import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    cursor.execute("SELECT COUNT(*), SUM(\"StockValue\") FROM JIVO_BEVERAGES_HANADB.OITW WHERE \"WhsCode\"='BH-FG' AND \"OnHand\">0")
    row = cursor.fetchone()
    print(f"JIVO_BEVERAGES Items in BH-FG: {row[0]}, Val: {row[1]}")
    c.close()
except Exception as e:
    print(e)
