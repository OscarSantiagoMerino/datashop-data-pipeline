# tests/test_transform.py
import pandas as pd
from scripts.transform import calculate_metrics
from io import StringIO

def make_orders_csv():
    s = """order_id,customer_id,product_id,quantity,unit_price,order_date
1,10,100,2,5.0,2025-11-13
2,11,101,1,10.0,2025-11-13
3,10,100,3,5.0,2025-11-13
"""
    return pd.read_csv(StringIO(s))

def make_customers_csv():
    s = """customer_id,customer_name,email
10,Alice,alice@example.com
11,Bob,bob@example.com
"""
    return pd.read_csv(StringIO(s))

def test_calculate_total_sales():
    orders = make_orders_csv()
    customers = make_customers_csv()
    metrics = calculate_metrics(orders, customers)
    # ventas: (2*5)+(1*10)+(3*5) = 10+10+15 = 35
    assert metrics['total_sales'] == 35.0

def test_find_top_product():
    orders = make_orders_csv()
    customers = make_customers_csv()
    metrics = calculate_metrics(orders, customers)
    # product 100: quantity 5, product 101: 1
    assert metrics['top_products'][0]['product_id'] == 100
    assert metrics['top_products'][0]['quantity'] == 5

def test_handle_empty_data():
    orders = pd.DataFrame(columns=['order_id','customer_id','product_id','quantity','unit_price','order_date'])
    customers = pd.DataFrame(columns=['customer_id','customer_name','email'])
    metrics = calculate_metrics(orders, customers)
    assert metrics['total_sales'] == 0.0
    assert metrics['top_products'] == []
