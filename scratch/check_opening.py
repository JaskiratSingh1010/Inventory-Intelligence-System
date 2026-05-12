import hdbcli.dbapi as dbapi
import pandas as pd

def check_opening():
    try:
        c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
        sql = """
        SELECT 
            N."Warehouse", 
            ROUND(SUM(CASE WHEN N."InQty">0 THEN N."InQty" ELSE 0 END) - 
                  SUM(CASE WHEN N."OutQty">0 THEN N."OutQty" ELSE 0 END), 0) AS "OpeningQty" 
        FROM JIVO_OIL_HANADB.OINM N 
        WHERE N."ItemCode"='PM0000584' 
          AND N."DocDate" < ADD_MONTHS(CURRENT_DATE, -1) 
        GROUP BY N."Warehouse"
        """
        df = pd.read_sql(sql, c)
        print(df)
        c.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_opening()
