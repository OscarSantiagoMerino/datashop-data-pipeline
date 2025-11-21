# scripts/transform.py
import pandas as pd
from pathlib import Path
from scripts.data_quality_check import check_orders_df, check_customers_df, DataQualityError

def load_csv(path):
    return pd.read_csv(path)

def calculate_metrics(orders_df: pd.DataFrame, customers_df: pd.DataFrame):
    # total ventas del día (quantity * unit_price)
    orders_df['line_total'] = orders_df['quantity'] * orders_df['unit_price']
    total_sales = orders_df['line_total'].sum()

    # top 5 productos por cantidad vendida
    top_products = (orders_df.groupby('product_id')['quantity']
                    .sum()
                    .reset_index()
                    .sort_values('quantity', ascending=False)
                    .head(5))

    # cliente con compra más grande (por monto total)
    customer_totals = (orders_df.groupby('customer_id')['line_total']
                       .sum()
                       .reset_index()
                       .sort_values('line_total', ascending=False))
    top_customer_id = customer_totals.iloc[0]['customer_id'] if not customer_totals.empty else None
    top_customer_amount = customer_totals.iloc[0]['line_total'] if not customer_totals.empty else 0

    top_customer_name = None
    if top_customer_id is not None:
        cust = customers_df[customers_df['customer_id'] == top_customer_id]
        top_customer_name = cust['customer_name'].iloc[0] if not cust.empty else None

    result = {
        'total_sales': float(total_sales),
        'top_products': top_products.to_dict(orient='records'),
        'top_customer': {
            'customer_id': int(top_customer_id) if top_customer_id is not None else None,
            'customer_name': top_customer_name,
            'amount': float(top_customer_amount)
        }
    }
    return result

def transform_pipeline(input_orders_csv, input_customers_csv, output_path):
    orders = load_csv(input_orders_csv)
    customers = load_csv(input_customers_csv)

    # validaciones
    try:
        check_orders_df(orders)
        check_customers_df(customers)
    except DataQualityError as e:
        raise

    metrics = calculate_metrics(orders, customers)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Guardar resumen en JSON y CSV para evidencias
    import json
    out.write_text(json.dumps(metrics, indent=2, ensure_ascii=False))
    # optional: guardar top_products en CSV
    pd.DataFrame(metrics['top_products']).to_csv(str(out.with_suffix('.top_products.csv')), index=False)
    return metrics

if __name__ == "__main__":
    import sys
    # usage: python scripts/transform.py data/input/orders_YYYY-MM-DD.csv data/input/customers.csv data/output/result_YYYY-MM-DD.json
    input_orders = sys.argv[1]
    input_customers = sys.argv[2]
    output_path = sys.argv[3]
    try:
        metrics = transform_pipeline(input_orders, input_customers, output_path)
        print("OK", metrics)
    except Exception as e:
        print("FALLÓ:", e)
        raise
