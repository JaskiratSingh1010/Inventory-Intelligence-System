import hdbcli.dbapi as dbapi
import pandas as pd

def check_fg_types():
    try:
        c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
        sql = """
        SELECT 
            N."TransType", 
            COUNT(*) AS "Count",
            SUM(N."InQty") AS "TotalIn"
        FROM JIVO_OIL_HANADB.OINM N 
        WHERE N."ItemCode"='FG0000030' 
          AND N."InQty" > 0
          AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE, -1)
          AND N."Warehouse" != 'BH-PP'
        GROUP BY N."TransType"
        """
        df = pd.read_sql(sql, c)
        print(df)
        c.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_fg_types()
