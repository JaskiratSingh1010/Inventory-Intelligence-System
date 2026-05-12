import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    cursor.execute("""
        SELECT COUNT(*), SUM(W.\"StockValue\")
        FROM JIVO_OIL_HANADB.OITW W 
        JOIN JIVO_OIL_HANADB.OITM M ON W.\"ItemCode\"=M.\"ItemCode\" 
        WHERE W.\"WhsCode\"='BH-FG' AND W.\"OnHand\">0 
        AND M.\"U_Sub_Group\"='GIFT PACK'
    """)
    row = cursor.fetchone()
    print(f"GIFT PACK items in BH-FG: {row[0]}, Val: {row[1]}")
    c.close()
except Exception as e:
    print(e)
