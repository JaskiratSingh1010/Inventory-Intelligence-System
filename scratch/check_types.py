import hdbcli.dbapi as dbapi
import pandas as pd

def check_types():
    try:
        c = dbapi.connect(address='192.168.1.182', port=30015, user='DATA1', password='Jivo@1989')
        sql = """
        SELECT 
            N."TransType", 
            COUNT(*) AS "Count",
            SUM(N."InQty") AS "TotalIn",
            SUM(N."OutQty") AS "TotalOut"
        FROM JIVO_OIL_HANADB.OINM N 
        WHERE N."ItemCode"='PM0000195' 
          AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE, -1)
        GROUP BY N."TransType"
        """
        df = pd.read_sql(sql, c)
        print(df)
        c.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_types()
