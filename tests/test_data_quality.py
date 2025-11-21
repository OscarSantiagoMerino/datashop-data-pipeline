# tests/test_data_quality.py
import pandas as pd
import pytest
from scripts.data_quality_check import check_orders_df, DataQualityError

def test_missing_columns():
    df = pd.DataFrame({'order_id':[1]})
    with pytest.raises(DataQualityError):
        check_orders_df(df)

def test_negative_quantity():
    df = pd.DataFrame({
        'order_id':[1],
        'customer_id':[1],
        'product_id':[1],
        'quantity':[-1],
        'unit_price':[10],
        'order_date':['2025-11-13']
    })
    with pytest.raises(DataQualityError):
        check_orders_df(df)
