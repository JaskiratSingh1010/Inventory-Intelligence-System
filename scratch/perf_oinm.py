import hdbcli.dbapi as dbapi
import time
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    s = time.time()
    # Test performance of OINM aggregation
    cursor.execute("""
        SELECT \"Warehouse\", ROUND(SUM(\"TransValue\"), 0) 
        FROM JIVO_OIL_HANADB.OINM 
        GROUP BY \"Warehouse\"
    """)
    rows = cursor.fetchall()
    print(f"OINM aggregation took {time.time()-s:.2f}s")
    for r in rows:
        if r[0] == 'BH-FG':
            print(f"BH-FG (OINM): {r[1]}")
    c.close()
except Exception as e:
    print(e)
