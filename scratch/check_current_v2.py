import hdbcli.dbapi as dbapi
import pandas as pd

def check_current():
    try:
        c = dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')
        sql = """
        SELECT 
            W."WhsCode", 
            W."OnHand"
        FROM JIVO_OIL_HANADB.OITW W
        WHERE W."ItemCode"='PM0000195' 
          AND W."OnHand" != 0
        """
        df = pd.read_sql(sql, c)
        print("--- Current On Hand by Warehouse ---")
        print(df)
        print("\nTotal (excluding BH-PP):", df[df['WhsCode'] != 'BH-PP']['OnHand'].sum())
        c.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_current()
