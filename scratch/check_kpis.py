import hdbcli.dbapi as dbapi
import pandas as pd

def check_kpis():
    try:
        c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
        # PURCHASE QTY
        sql_pur = """
        SELECT ROUND(SUM(N."InQty"), 0) AS "PurchaseQty"
        FROM JIVO_OIL_HANADB.OINM N
        WHERE N."ItemCode"='PM0000195' 
          AND N."TransType" IN (20, 67) 
          AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE, -1)
          AND N."Warehouse" != 'BH-PP'
        """
        # PRODUCTION QTY (CONSUMPTION)
        sql_prod = """
        SELECT ROUND(SUM(N."OutQty"), 0) AS "ProductionQty"
        FROM JIVO_OIL_HANADB.OINM N
        WHERE N."ItemCode"='PM0000195' 
          AND N."TransType" IN (60, 202) 
          AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE, -1)
          AND N."Warehouse" != 'BH-PP'
        """
        
        pur = pd.read_sql(sql_pur, c).iloc[0]['PurchaseQty']
        prod = pd.read_sql(sql_prod, c).iloc[0]['ProductionQty']
        
        print(f"Purchased Qty (Dashboard Logic): {pur}")
        print(f"Production Qty (Dashboard Logic): {prod}")
        c.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_kpis()
