import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    # Check for items without a valid group name or in other groups
    cursor.execute("""
        SELECT COALESCE(G.\"ItmsGrpNam\", 'NO GROUP'), COUNT(*), SUM(W.\"OnHand\"), SUM(W.\"StockValue\")
        FROM JIVO_OIL_HANADB.OITW W 
        JOIN JIVO_OIL_HANADB.OITM M ON W.\"ItemCode\"=M.\"ItemCode\" 
        LEFT JOIN JIVO_OIL_HANADB.OITB G ON M.\"ItmsGrpCod\"=G.\"ItmsGrpCod\" 
        WHERE W.\"WhsCode\"='BH-FG' AND W.\"OnHand\">0 
        GROUP BY G.\"ItmsGrpNam\"
    """)
    rows = cursor.fetchall()
    for r in rows:
        print(f"Group {r[0]}: {r[1]} items, Qty: {r[2]}, Val: {r[3]}")
    c.close()
except Exception as e:
    print(e)
