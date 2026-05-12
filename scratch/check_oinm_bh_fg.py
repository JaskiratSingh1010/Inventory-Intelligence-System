import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    # Count items and sum value using OINM (Audit Trail)
    cursor.execute("""
        SELECT COUNT(DISTINCT \"ItemCode\"), ROUND(SUM(\"InQty\" - \"OutQty\"), 0) as Qty, ROUND(SUM(\"TransValue\"), 0) as Val
        FROM JIVO_OIL_HANADB.OINM 
        WHERE \"Warehouse\"='BH-FG'
    """)
    row = cursor.fetchone()
    print(f"OINM Items in BH-FG: {row[0]}, Qty: {row[1]}, Val: {row[2]}")
    c.close()
except Exception as e:
    print(e)
