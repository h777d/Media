"""
@author: hosseind
"""


from fastapi import FastAPI, Query
import sqlite3
import uvicorn
import json
import os


# Step1: Extract
# CREATE TABLE product_details (
#    ProductID INTEGER PRIMARY KEY,
#    ProductName TEXT,
#    Category TEXT);
# ...


app = FastAPI() # web API

DATABASE_PATH = os.path.abspath(os.path.join(os.path.dirname('/Users/hosseind/Downloads/case/'),'SQLdb.db'))

def execute_query(query):
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return result

def execute_script(script):
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()
    cursor.executescript(script)
    result = cursor.fetchall()
    connection.close()
    return result



SELECT COALESCE(ProductID, (SELECT AVG(Quantity) FROM sales_A)) AS Quantity,
FROM sales_A
SET Quantity = (SELECT AVG(Quantity) FROM sales_A WHERE Quantity IS NOT NULL)
    WHERE Quantity IS NULL;

# Step2: Cleaning
@app.get("/clean")
def clean_sales():
    query = """
    UPDATE sales_A
    SET Quantity = (SELECT AVG(Quantity) FROM sales_A WHERE Quantity IS NOT NULL)
    WHERE Quantity IS NULL;

    UPDATE sales_A
    SET Price = (SELECT AVG(Price) FROM sales_A WHERE Price IS NOT NULL)
    WHERE Price IS NULL;

    UPDATE sales_A
    SET ProductID = (SELECT COALESCE(ProductID, LEAD(ProductID) OVER (ORDER BY Date)) 
    FROM sales_A)
    WHERE ProductID IS NULL;

    UPDATE sales_B
    SET Quantity = (SELECT AVG(Quantity) FROM sales_B WHERE Quantity IS NOT NULL)
    WHERE Quantity IS NULL;

    UPDATE sales_B
    SET Price = (SELECT AVG(Price) FROM sales_B WHERE Price IS NOT NULL)
    WHERE Price IS NULL;

    UPDATE sales_B
    SET ProductID = (SELECT COALESCE(ProductID, LEAD(ProductID) OVER (ORDER BY Date)) 
    FROM sales_B)
    WHERE ProductID IS NULL;
    """
    execute_script(query)
    sales_A = execute_query("SELECT * FROM sales_A LIMIT 10;")
    return {"updated_sales_A": sales_A}  # JSON format


# Step3: Transform 
@app.get("/transform")
def transform_sales_data():
    query = """
    ALTER TABLE sales_A ADD COLUMN "Year" INTEGER;
    ALTER TABLE sales_A ADD COLUMN Month INTEGER;
    
    ALTER TABLE sales_B ADD COLUMN "Year" INTEGER;
    ALTER TABLE sales_B ADD COLUMN Month INTEGER;


    UPDATE sales_B
    SET Date = 
        DATETIME(
            SUBSTR(Date, 7, 4) || '-' ||  -- Year
            SUBSTR(Date, 4, 2) || '-' ||  -- Month
            SUBSTR(Date, 1, 2)             -- Day
        );

    UPDATE sales_B
    SET Year = SUBSTR(Date, 1, 4);   
    UPDATE sales_B
    SET Month = SUBSTR(Date, 6, 2);   

    
    UPDATE sales_A
    SET Date = 
        DATETIME(
            SUBSTR(Date, 7, 4) || '-' ||  
            SUBSTR(Date, 1, 2) || '-' ||  
            SUBSTR(Date, 4, 2)            
        );

    UPDATE sales_A
    SET Year = SUBSTR(Date, 1, 4);
    UPDATE sales_A
    SET Month = SUBSTR(Date, 6, 2);
    """
    execute_script(query)
    updated_sales_A = execute_query("SELECT TransactionID, Date, Year, Month FROM sales_A LIMIT 10;")
    updated_sales_B = execute_query("SELECT TransactionID, Date, Year, Month FROM sales_B LIMIT 10;")
    return {"updated_sales_A [TransactionID, Date, Year, Month]": updated_sales_A, 
            "updated_sales_B [TransactionID, Date, Year, Month]": updated_sales_B}


# Step4: Enrich
@app.get("/enrich")
def enrich_sales():
    query = """
    CREATE TABLE sales AS
    SELECT * FROM sales_A
    WHERE 0;

    INSERT INTO sales
    SELECT * FROM sales_A;

    INSERT INTO sales
    SELECT * FROM sales_B;

    CREATE TABLE enriched_sales AS
    SELECT S.TransactionID, S.Date, S.Year, S.Month, S.ProductID, S.Quantity, S.Price, P.ProductName, P.Category
    FROM sales S
    JOIN product_ids P ON S.ProductID = P.ProductID;
    """
    execute_script(query)
    
    head = execute_query("""SELECT name FROM PRAGMA_TABLE_INFO('enriched_sales');""")
    enriched_sales = execute_query("SELECT * FROM enriched_sales LIMIT 15;")
    return {"Columns names:": head, "enriched_sales": enriched_sales}


# Step 5: calculate total sales
@app.get("/total_sales")
def calculate_total_sales():
    query = """
    ALTER TABLE enriched_sales ADD COLUMN Total_Sales REAL; 
    UPDATE enriched_sales
    SET Total_Sales = Quantity * Price;
    """
    execute_script(query)
    
    enriched_sales = execute_query("SELECT * FROM enriched_sales LIMIT 10;")
    return {"enriched_sales [TransactionID, ProductID, Year, Month, ProductName, Category, Total_Sales]": enriched_sales}


# Step 6: aggregation
@app.get("/aggregation")
def aggregate_sales():
    query = """
    CREATE VIEW agg_sales_YM AS
    SELECT Year, Month, Category, SUM(Total_Sales) AS Total_Sales
    FROM enriched_sales
    GROUP BY Year, Month, Category;

    CREATE VIEW agg_sales_C AS
    SELECT Year, Month, Category, SUM(Total_Sales) AS Total_Sales
    FROM enriched_sales
    GROUP BY Year, Month, Category;
    """
    execute_script(query)

    agg_sales_YM = execute_query("SELECT * FROM agg_sales_YM;")
    agg_sales_C = execute_query("SELECT * FROM agg_sales_C;")
    return {"aggregated sales per Year and Month [Year, Month, Total_Sales]": agg_sales_YM, 
            "aggregated sales per Category [Category, Total_Sales]": agg_sales_C}




if __name__ == "__main__":

    uvicorn.run(app, host="127.0.0.1", port=8000)

# uvicorn sales_API:app --reload