USE DATABASE MYDB;

USE SCHEMA GOLD;

select * from final_fact;

select * from dim_walmart_store;

select * from dim_walmart_date;

-- weekly sales by store and holiday

SELECT
    f.STORE_ID,
    d.ISHOLIDAY,
    SUM(f.STORE_WEEKLY_SALES)
FROM DIM_WALMART_DATE d
LEFT JOIN FINAL_FACT f
ON f.DATE_ID = d.DATE_ID
GROUP BY 1,2;

-- weekly sales by temperature and year

SELECT
  f.TEMPERATURE,
  YEAR(d.STORE_DATE) AS YEAR,
  SUM(f.STORE_WEEKLY_SALES) AS Total_Sales
FROM final_fact f
JOIN DIM_WALMART_DATE d
ON d.DATE_ID = f.date_id
GROUP BY 1,2;

-- weekly sales by store size
SELECT
  s.STORE_SIZE,
  SUM(f.STORE_WEEKLY_SALES) AS Total_Sales
FROM FINAL_FACT f
JOIN dim_walmart_store s 
ON s.STORE_ID = f.store_id
GROUP BY 1;


-- Markdown sales by year and store
SELECT
    f.STORE_ID,
    SUM(f.MARKDOWN1),
    SUM(f.MARKDOWN2),
    SUM(f.MARKDOWN3),
    SUM(f.MARKDOWN4),
    SUM(f.MARKDOWN5),
    YEAR(d.STORE_DATE) AS Year
FROM DIM_WALMART_DATE d
JOIN FINAL_FACT f
ON d.DATE_ID = f.DATE_ID
GROUP BY 1,7;

-- weekly sales by store type
SELECT
  s.STORE_TYPE,
  SUM(f.STORE_WEEKLY_SALES) AS Total_Sales
FROM FINAL_FACT f
JOIN dim_walmart_store s 
ON s.STORE_ID = f.store_id
GROUP BY 1;

-- Fuel price by year
SELECT
  s.STORE_ID,
  SUM(f.FUEL_PRICE),
  YEAR(d.STORE_DATE) AS YEAR
FROM final_fact f
JOIN DIM_WALMART_DATE d
ON d.DATE_ID = f.date_id
JOIN DIM_WALMART_STORE s
ON s.STORE_ID = f.STORE_ID
GROUP BY 1,3;

-- Weekly sales by CPI

SELECT
  CPI,
  SUM(STORE_WEEKLY_SALES) AS Total_Sales
FROM final_fact
GROUP BY 1