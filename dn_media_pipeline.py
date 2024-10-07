"""
@author: hosseind
"""

import pandas as pd
import sqlite3
import argparse
import os
import logging
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt
import pickle

import config
import utils


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/Users/hosseind/Downloads/case/data_pipeline.log'),
        logging.StreamHandler()
    ]
)
logging.basicConfig(level=logging.DEBUG)


def clean_data(sales_A, sales_B):
    try:
        sales = pd.concat([sales_A, sales_B], ignore_index=True)
        sales['Quantity'].fillna(sales['Quantity'].mean().round(), inplace=True)
        sales['Price'].fillna(sales['Price'].mean().round(), inplace=True)
        sales['ProductID'].fillna(sales['ProductID'].interpolate(), inplace=True)
        
        sales['Date'] = pd.to_datetime(sales['Date'], format='%m/%d/%Y', errors='coerce').combine_first(
            pd.to_datetime(sales['Date'], format='%d-%m-%Y', errors='coerce'))
        sales['Year'] = sales['Date'].dt.year
        sales['Month'] = sales['Date'].dt.month
        logging.info("Step 2&3: Data cleaned & transformed successfully. Sales_data:")
        sales.head()
        return sales
    except Exception as e:
        logging.error(f"Error during data cleaning & transforming: {e}")
        raise

def enrich_data(sales, product_ids):
    try:
        sales = sales.merge(product_ids, on='ProductID', how='left')
        sales['TotalSales'] = sales['Quantity'] * sales['Price']
        logging.info("Step4: Data enriched successfully. Enriched data:")
        sales.head()
        return sales
    except Exception as e:
        logging.error(f"Error during data enrichment: {e}")
        raise

def aggregate_data(sales):
    try:
        report_cat = sales.groupby(['Category'])['TotalSales'].sum().reset_index()
        report_date = sales.groupby(['Year', 'Month'])['TotalSales'].sum().reset_index()
        logging.info("Step5: Data aggregated successfully. Aggregated_data:")
        return report_cat, report_date
    except Exception as e:
        logging.error(f"Error during data aggregation: {e}")
        sales.head()
        raise

def load_to_db(db_path, report_cat, report_date):
    try:
        connect = sqlite3.connect(db_path)
        report_cat.to_sql('report_cat', connect, if_exists='replace', index=False)
        report_date.to_sql('report_date', connect, if_exists='replace', index=False)
        connect.close()
        logging.info("Step6: Data loaded to database successfully. reports:")
        print(report_cat)
        print(report_date)
    except Exception as e:
        logging.error(f"Error loading data into database: {e}")
        raise




def run_arima_forecast(sales):
    try:
        sales2 = sales.copy()
        sales2.set_index('Date', inplace=True)
        monthly_sales = sales2['TotalSales'].resample('M').sum()

        model = ARIMA(monthly_sales, order=config.ARIMA_ORDER)
        arima_result = model.fit()

        if config.save_model:
            with open(config.model_path, 'wb') as f:
                pickle.dump(arima_result, f)
                
        forecast = arima_result.forecast(steps=config.FORECAST_STEPS)
    
        plt.plot(monthly_sales.index, monthly_sales, label='Observed')
        plt.plot(forecast.index, forecast, label='Forecast', color='red')
        plt.legend()
        plt.savefig(config.Plot_path)
    
        return forecast
    except Exception as e:
        logging.error(f"Error training ARIMA model: {e}")
        raise e




def main(sales_A_path, sales_B_path, product_ids_path, db_path):
    try:
        # Step 1: Extract data
        logging.info(f"Step 1: Extract data")
        sales_A = utils.load_csv(sales_A_path)
        sales_B = utils.load_csv(sales_B_path)
        product_ids = utils.load_csv(product_ids_path)

        # Step 2: Clean data
        sales = clean_data(sales_A, sales_B)

        # Step 3: Enrich data
        sales = enrich_data(sales, product_ids)

        # Step 4: Aggregate data
        report_cat, report_date = aggregate_data(sales)

        # Step 5: Load to database
        load_to_db(db_path, report_cat, report_date)

        # Step 6: Fetch and print database info
        df = utils.db_info(db_path)
        print(df.head())

        # Step ML: Forecast Total Sales
        forecast = run_arima_forecast(sales)
        logging.info("ARIMA forecasting completed.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DN Media Pipeline")
    parser.add_argument("--sales_A", required=True, help="Path to sales_region_A.csv")
    parser.add_argument("--sales_B", required=True, help="Path to sales_region_B.csv")
    parser.add_argument("--product_ids", required=True, help="Path to product_details.csv")
    parser.add_argument("--database", required=True, help="Path to database sales.db")

    args = parser.parse_args()
    
    main(args.sales_A, args.sales_B, args.product_ids, args.database)
