from prefect import flow, task
import os
import pandas as pd

INPUT_FOLDER = "data/input/"
OUTPUT_FOLDER = "data/output/"

@task
def verificar_datos_fuente():
    if not os.path.exists(INPUT_FOLDER + "orders_2025-11-13.csv"):
        raise FileNotFoundError("No existe orders_2025-11-13.csv")
    if not os.path.exists(INPUT_FOLDER + "customers.csv"):
        raise FileNotFoundError("No existe customers.csv")
    return "Datos encontrados"

@task
def ejecutar_control_calidad():
    df = pd.read_csv(INPUT_FOLDER + "orders_2025-11-13.csv")
    if df["order_id"].isnull().any():
        raise ValueError("order_id tiene valores nulos")
    if (df["quantity"] <= 0).any():
        raise ValueError("quantity inválido")
    return "Quality OK"

@task
def transformar_datos():
    df = pd.read_csv(INPUT_FOLDER + "orders_2025-11-13.csv")
    df["total"] = df["quantity"] * df["unit_price"]

    resultado = {
        "ventas_totales": df["total"].sum(),
        "top_5_productos": df.groupby("product_id")["quantity"].sum().nlargest(5).to_dict(),
        "cliente_mayor_compra": df.loc[df["total"].idxmax()]["customer_id"]
    }

    output_path = OUTPUT_FOLDER + "resultados.json"
    pd.Series(resultado).to_json(output_path)
    return output_path

@task
def cargar_resultado(path):
    return f"Resultado cargado en {path}"

@flow(name="DataShop Daily Pipeline")
def datashop_daily_pipeline():
    verificar_datos_fuente()
    ejecutar_control_calidad()
    path = transformar_datos()
    cargar_resultado(path)

if __name__ == "__main__":
    datashop_daily_pipeline()
