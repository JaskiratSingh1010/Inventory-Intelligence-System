-- Debug Query for Item FG0000175 Stock Value Issue
-- Dashboard shows: ₹68,209
-- SAP shows: ₹43,269

-- 1. Check StockValue field directly (Dashboard Method)
SELECT 
    'Dashboard Method (W.StockValue)' AS Method,
    W."WhsCode",
    W."OnHand",
    W."StockValue" AS StockValue_Field,
    M."LastPurPrc" AS LastPrice,
    (W."OnHand" * M."LastPurPrc") AS Calculated_LastPrice
FROM JIVO_OIL_HANADB.OITW W 
JOIN JIVO_OIL_HANADB.OITM M ON W."ItemCode" = M."ItemCode" 
WHERE W."ItemCode" = 'FG0000175' AND W."OnHand" > 0;

-- 2. Check aggregation across warehouses (Dashboard Method)
SELECT 
    'Dashboard Aggregated' AS Method,
    COUNT(*) AS Warehouse_Count,
    SUM(W."OnHand") AS TotalOnHand,
    SUM(W."StockValue") AS TotalStockValue,
    SUM(W."OnHand" * M."LastPurPrc") AS TotalCalculated
FROM JIVO_OIL_HANADB.OITW W 
JOIN JIVO_OIL_HANADB.OITM M ON W."ItemCode" = M."ItemCode" 
WHERE W."ItemCode" = 'FG0000175' AND W."OnHand" > 0;

-- 3. Check SAP OITW.StockValue vs OINM transaction-based value
SELECT 
    'SAP Comparison' AS Method,
    SUM(W."StockValue") AS OITW_StockValue,
    SUM(N."TransValue") AS OINM_TransValue
FROM JIVO_OIL_HANADB.OITW W 
JOIN JIVO_OIL_HANADB.OITM M ON W."ItemCode" = M."ItemCode" 
LEFT JOIN JIVO_OIL_HANADB.OINM N ON W."ItemCode" = N."ItemCode" AND W."WhsCode" = N."Warehouse"
WHERE W."ItemCode" = 'FG0000175' AND W."OnHand" > 0;

-- 4. Check if there are multiple databases for beverages
-- (If this is beverages item, check all DBs)
SELECT 
    'Multi-DB Check' AS Method,
    'JIVO_OIL_HANADB' AS Database,
    SUM(W."StockValue") AS TotalStockValue
FROM JIVO_OIL_HANADB.OITW W 
WHERE W."ItemCode" = 'FG0000175' AND W."OnHand" > 0

UNION ALL

SELECT 
    'Multi-DB Check' AS Method,
    'JIVO_BEVERAGES_HANADB' AS Database,
    SUM(W."StockValue") AS TotalStockValue
FROM JIVO_BEVERAGES_HANADB.OITW W 
WHERE W."ItemCode" = 'FG0000175' AND W."OnHand" > 0;
