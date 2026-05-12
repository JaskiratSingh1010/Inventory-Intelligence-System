import hdbcli.dbapi as dbapi
import os
try:
    c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
    cursor = c.cursor()
    cursor.execute("SELECT COUNT(*) FROM JIVO_OIL_HANADB.OITW W JOIN JIVO_OIL_HANADB.OITM M ON W.\"ItemCode\"=M.\"ItemCode\" WHERE W.\"WhsCode\"='GP-FG' AND W.\"OnHand\">0 AND (M.\"U_Unit\" != 'OIL' OR M.\"U_Unit\" IS NULL)")
    print(cursor.fetchone()[0])
    c.close()
except Exception as e:
    print(e)
