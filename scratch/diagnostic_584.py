import hdbcli.dbapi as dbapi
import pandas as pd

def full_diagnostic():
    try:
        c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
        
        # 1. Opening Stock (30 days ago)
        sql_open = """
        SELECT ROUND(SUM(CASE WHEN N."InQty">0 THEN N."InQty" ELSE 0 END) - 
                     SUM(CASE WHEN N."OutQty">0 THEN N."OutQty" ELSE 0 END), 0) AS "Value"
        FROM JIVO_OIL_HANADB.OINM N 
        WHERE N."ItemCode"='PM0000584' AND N."DocDate" < ADD_MONTHS(CURRENT_DATE, -1) AND N."Warehouse" != 'BH-PP'
        """
        
        # 2. Purchases (Last 30 days: 20, 59, 202)
        sql_pur = """
        SELECT ROUND(SUM(N."InQty"), 0) AS "Value"
        FROM JIVO_OIL_HANADB.OINM N 
        WHERE N."ItemCode"='PM0000584' AND N."TransType" IN (20, 59, 202) AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE, -1) AND N."Warehouse" != 'BH-PP'
        """
        
        # 3. Consumption (Last 30 days: 60, 202)
        sql_con = """
        SELECT ROUND(SUM(N."OutQty"), 0) AS "Value"
        FROM JIVO_OIL_HANADB.OINM N 
        WHERE N."ItemCode"='PM0000584' AND N."TransType" IN (60, 202) AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE, -1) AND N."Warehouse" != 'BH-PP'
        """
        
        # 4. Closing Stock (KPI Card)
        sql_cls = """
        SELECT ROUND(SUM(W."OnHand"), 0) AS "Value"
        FROM JIVO_OIL_HANADB.OITW W 
        WHERE W."ItemCode"='PM0000584' AND W."WhsCode" != 'BH-PP'
        """
        
        # 5. Warehouse Table Breakdown
        sql_whs = """
        SELECT W."WhsCode", W."OnHand"
        FROM JIVO_OIL_HANADB.OITW W 
        WHERE W."ItemCode"='PM0000584' AND W."OnHand" != 0 AND W."WhsCode" != 'BH-PP'
        """
        
        print("--- KPI SUMMARY ---")
        print("Opening Stock:", pd.read_sql(sql_open, c).iloc[0]['Value'])
        print("Purchased:", pd.read_sql(sql_pur, c).iloc[0]['Value'])
        print("Consumed:", pd.read_sql(sql_con, c).iloc[0]['Value'])
        print("Current Stock (Card):", pd.read_sql(sql_cls, c).iloc[0]['Value'])
        
        print("\n--- WAREHOUSE TABLE ---")
        df_whs = pd.read_sql(sql_whs, c)
        print(df_whs)
        print("Table Total:", df_whs['OnHand'].sum())
        
        c.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    full_diagnostic()
