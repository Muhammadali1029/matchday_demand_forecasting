from pathlib import Path
import pandas as pd

from loguru import logger
from tqdm import tqdm
import typer

from matchday_demand_forecasting.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

RAW_FILES = ['ORDER_DETAILS.xlsx', 'ORDER_HEADER.xlsx', 'PRODUCTS.xlsx']

def precess_raw_data(raw_data_dir: Path, output_path: Path, products_output_path: Path):
    """
    Loads, cleans, transforms and merges the raw sales data.
    """

    logger.info(f"Processing raw data in {raw_data_dir}...")

    # Load the raw data
    data_paths = [raw_data_dir / file for file in RAW_FILES]
    try:
        df_1 = pd.read_excel(data_paths[0]) # ORDER DETAILS
        df_2 = pd.read_excel(data_paths[1]) # ORDER HEADERS
        df_3 = pd.read_excel(data_paths[2]) # PRODUCTS
        logger.info("Raw data loaded successfully.")
    except FileNotFoundError as e:
        logger.error(f"Error loading raw data: {e}")
        raise e
    
    # --- PROCESSING ORDER DETAILS (df_1) ---
    logger.info("Processing ORDER DETAILS...")
    df_1_filtered = df_1[['ORDER_HEADER_ID', 'PRODUCT_ID', 'QTY', 'SUB_TOTAL']].copy()

    # --- Cleaning the SUB_TOTAL cloumn ---
    # Replace the comma with period and convert to float
    df_1_filtered['SUB_TOTAL'] = df_1_filtered['SUB_TOTAL'].astype(str).str.replace(',', '.', regex=True).astype(float)
    logger.info("Cleaned 'SUB_TOTAL' column to float type.")

    # --- PROCESSING ORDER HEADERS (df_2) ---
    logger.info("Processing ORDER HEADERS...")
    df_2_filtered = df_2[['ORDER_ID', 'ORDER_DATE', 'PAYMENT_METHOD']].copy()

    # Convert and split ORDER_DATE
    df_2_filtered['ORDER_DATE'] = pd.to_datetime(df_2_filtered['ORDER_DATE'], dayfirst=True)
    df_2_filtered['ORDER_DATE_ONLY'] = df_2_filtered['ORDER_DATE'].dt.date
    df_2_filtered['ORDER_TIME_ONLY'] = df_2_filtered['ORDER_DATE'].dt.time
    df_2_filtered = df_2_filtered.drop('ORDER_DATE', axis=1)
    logger.info("Split 'ORDER_DATE' into 'ORDER_DATE_ONLY' and 'ORDER_TIME_ONLY' columns.")

    # --- MERGING AND FINAL FILTERING ---
    # Merge the two tables 
    df_final = pd.merge(df_1_filtered, df_2_filtered, left_on='ORDER_HEADER_ID', right_on='ORDER_ID', how='inner')
    df_final = df_final.drop('ORDER_HEADER_ID', axis=1)
    logger.info(f"Merged datasets. Current shape: {df_final.shape}")

    # Remove all 0 quantity rows
    df_final = df_final[df_final['QTY'] > 0].copy()
    logger.info(f"Removed 0 quantity rows. Final shape: {df_final.shape}")

    # --- PROCESSING PRODUCTS (df_3) ---
    logger.info("Processing PRODUCTS...")
    df_products = df_3[['PRODUCT_ID', 'PRODUCT_NAME']].copy()


    # SAVE THE PROCESSED DATA 
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df_final.to_csv(output_path, index=False)
    logger.success(f"Processed FINAL SALES DATA saved to {output_path}.")

    df_products.to_csv(products_output_path, index=False)
    logger.success(f"Processed PRODUCTS DATA saved to {products_output_path}.")

    return df_final, df_products


# Typer command 
@app.command()
def main(
    raw_data_dir: Path = RAW_DATA_DIR,
    output_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
    products_output_path: Path = PROCESSED_DATA_DIR / "products.csv",
):
    # Call the new processing function 
    precess_raw_data(raw_data_dir, output_path, products_output_path)

if __name__ == "__main__":
    app()

    

