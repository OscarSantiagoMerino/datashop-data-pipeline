# scripts/data_quality_check.py
import pandas as pd

REQUIRED_ORDER_COLS = {'order_id','customer_id','product_id','quantity','unit_price','order_date'}
REQUIRED_CUSTOMER_COLS = {'customer_id','customer_name','email'}

class DataQualityError(Exception):
    pass

def check_orders_df(df: pd.DataFrame):
    # columnas
    if not REQUIRED_ORDER_COLS.issubset(set(df.columns)):
        missing = REQUIRED_ORDER_COLS - set(df.columns)
        raise DataQualityError(f"Faltan columnas en orders: {missing}")
    # nulos importantes
    if df['order_id'].isnull().any() or df['customer_id'].isnull().any():
        raise DataQualityError("order_id o customer_id contienen nulos")
    # positivos
    if (df['quantity'] <= 0).any() or (df['unit_price'] <= 0).any():
        raise DataQualityError("quantity y unit_price deben ser > 0")
    return True

def check_customers_df(df: pd.DataFrame):
    if not REQUIRED_CUSTOMER_COLS.issubset(set(df.columns)):
        missing = REQUIRED_CUSTOMER_COLS - set(df.columns)
        raise DataQualityError(f"Faltan columnas en customers: {missing}")
    return True
