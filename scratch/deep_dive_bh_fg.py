import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    # Count unique ItemCodes in BH-FG with OnHand > 0
    cursor.execute("SELECT COUNT(DISTINCT \"ItemCode\") FROM JIVO_OIL_HANADB.OITW WHERE \"WhsCode\"='BH-FG' AND \"OnHand\">0")
    count = cursor.fetchone()[0]
    
    # Sum StockValue in BH-FG for items with OnHand > 0
    cursor.execute("SELECT ROUND(SUM(\"StockValue\"),0) FROM JIVO_OIL_HANADB.OITW WHERE \"WhsCode\"='BH-FG' AND \"OnHand\">0")
    val = cursor.fetchone()[0]
    
    print(f"Items in BH-FG (OnHand > 0): {count}")
    print(f"Total StockValue in BH-FG: {val}")
    
    # Check if there are items with OnHand > 0 but StockValue = 0
    cursor.execute("SELECT COUNT(*) FROM JIVO_OIL_HANADB.OITW WHERE \"WhsCode\"='BH-FG' AND \"OnHand\">0 AND \"StockValue\"=0")
    zero_val_count = cursor.fetchone()[0]
    print(f"Items with OnHand > 0 but StockValue = 0: {zero_val_count}")
    
    c.close()
except Exception as e:
    print(e)
