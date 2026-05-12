import hdbcli.dbapi as dbapi
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    # Check for items with decimal quantities in BH-GR
    cursor.execute("SELECT \"ItemCode\", \"OnHand\" FROM JIVO_OIL_HANADB.OITW WHERE \"WhsCode\"='BH-GR' AND \"OnHand\" > 0 AND \"OnHand\" != ROUND(\"OnHand\", 0)")
    rows = cursor.fetchall()
    print(f"Items with decimals in BH-GR: {len(rows)}")
    for r in rows[:5]:
        print(f"{r[0]}: {r[1]}")
    c.close()
except Exception as e:
    print(e)
