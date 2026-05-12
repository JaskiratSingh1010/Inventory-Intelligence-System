import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    cursor.execute("""
        SELECT G."ItmsGrpNam", COUNT(*), ROUND(SUM(W."StockValue"),0)
        FROM JIVO_OIL_HANADB.OITW W 
        JOIN JIVO_OIL_HANADB.OITM M ON W."ItemCode"=M."ItemCode" 
        JOIN JIVO_OIL_HANADB.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" 
        WHERE W."WhsCode\"='BH-FG' AND W.\"OnHand\">0 
        GROUP BY G."ItmsGrpNam"
    """)
    rows = cursor.fetchall()
    total_items = 0
    total_val = 0
    for r in rows:
        print(f"{r[0]}: {r[1]} items, Val: {r[2]}")
        total_items += r[1]
        total_val += r[2]
    print(f"TOTAL: {total_items} items, Val: {total_val}")
    c.close()
except Exception as e:
    print(e)
