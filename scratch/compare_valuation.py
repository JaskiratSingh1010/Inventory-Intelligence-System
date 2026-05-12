import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    # Check total value in BH-FG using OITW.StockValue
    cursor.execute("SELECT ROUND(SUM(\"StockValue\"),0) FROM JIVO_OIL_HANADB.OITW WHERE \"WhsCode\"='BH-FG' AND \"OnHand\">0")
    oitw_val = cursor.fetchone()[0]
    
    # Check total value in BH-FG using OnHand * LastPurPrc
    cursor.execute("""
        SELECT ROUND(SUM(W.\"OnHand\" * M.\"LastPurPrc\"), 0) 
        FROM JIVO_OIL_HANADB.OITW W 
        JOIN JIVO_OIL_HANADB.OITM M ON W.\"ItemCode\"=M.\"ItemCode\" 
        WHERE W.\"WhsCode\"='BH-FG' AND W.\"OnHand\">0
    """)
    calc_val = cursor.fetchone()[0]
    
    # Check total value in BH-FG using OnHand * AvgPrice (if available)
    cursor.execute("""
        SELECT ROUND(SUM(W.\"OnHand\" * W.\"AvgPrice\"), 0) 
        FROM JIVO_OIL_HANADB.OITW W 
        WHERE W.\"WhsCode\"='BH-FG' AND W.\"OnHand\">0
    """)
    avg_val = cursor.fetchone()[0]
    
    print(f"OITW.StockValue: {oitw_val}")
    print(f"OnHand * LastPurPrc: {calc_val}")
    print(f"OnHand * AvgPrice: {avg_val}")
    
    c.close()
except Exception as e:
    print(e)
