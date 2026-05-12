import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    cursor.execute("""
        SELECT G.\"ItmsGrpNam\", COUNT(*), SUM(W.\"StockValue\")
        FROM JIVO_BEVERAGES_HANADB.OITW W 
        JOIN JIVO_BEVERAGES_HANADB.OITM M ON W.\"ItemCode\"=M.\"ItemCode\" 
        JOIN JIVO_BEVERAGES_HANADB.OITB G ON M.\"ItmsGrpCod\"=G.\"ItmsGrpCod\" 
        WHERE W.\"WhsCode\"='BH-FG' AND W.\"OnHand\">0
        GROUP BY G.\"ItmsGrpNam\"
    """)
    rows = cursor.fetchall()
    for r in rows:
        print(f"Bev Group {r[0]}: {r[1]} items, Val: {r[2]}")
    c.close()
except Exception as e:
    print(e)
