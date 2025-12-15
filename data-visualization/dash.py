import io
import time
import pandas as pd
import streamlit as st
from hdfs import InsecureClient
import pyarrow.parquet as pq

WEBHDFS = "http://localhost:9870"  # ajusta si usas otro host/puerto
HDFS_USER = "hdfs"                 # o el usuario que uses
BASE = "/user/luis-goncalves/ecommerce"
REFRESH_EVERY = 10
NUM_PARTITIONS = 1

client = InsecureClient(WEBHDFS, user=HDFS_USER)

def list_partitions(path):
    files = client.list(path)  # devuelve nombres como 'fecha=2025-12-15'
    parts = sorted([f for f in files if f.startswith("fecha=")], reverse=True)
    return parts[:NUM_PARTITIONS]

def read_parquet_dir(path):
    # Lee todos los .parquet en el directorio dado usando WebHDFS (streams no seekables)
    dfs = []
    for fname in client.list(path):
        if fname.endswith(".parquet"):
            with client.read(f"{path}/{fname}") as f:
                buf = io.BytesIO(f.read())  # descargar a memoria para que pyarrow pueda hacer seek
                table = pq.read_table(buf)
                dfs.append(table.to_pandas())
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def load_orders():
    parts = list_partitions(f"{BASE}/orders")
    dfs = [read_parquet_dir(f"{BASE}/orders/{p}") for p in parts]
    df = pd.concat(dfs, ignore_index=True)
    if "window" in df.columns:
        df["minute"] = df["window"].apply(lambda w: w["start"])
    elif "event_time" in df.columns:
        df["minute"] = df["event_time"]
    # Normalizar a datetime y formatear a hh:mm:ss para el eje
    df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
    df["minute_str"] = df["minute"].dt.strftime("%H:%M:%S")
    return (df.groupby(["minute", "minute_str"], as_index=False)["total_price"].sum()
              .sort_values("minute"))

def load_cart_events():
    parts = list_partitions(f"{BASE}/cart_events")
    df = pd.concat([read_parquet_dir(f"{BASE}/cart_events/{p}") for p in parts], ignore_index=True)
    return (df.groupby(["category", "event_type"], as_index=False)["count"].sum()
              .sort_values("count", ascending=False))

def load_page_views():
    parts = list_partitions(f"{BASE}/page_views")
    df = pd.concat([read_parquet_dir(f"{BASE}/page_views/{p}") for p in parts], ignore_index=True)
    df["product_label"] = df["category"].fillna("cat") + " - " + df["product_id"].astype(str)
    return (df.groupby(["product_label", "category"], as_index=False)["count"].sum()
              .sort_values("count", ascending=False)
              .head(20))

st.set_page_config(page_title="Ecommerce Streaming", layout="wide")
st.title("Dash en vivo desde HDFS (WebHDFS)")

placeholder = st.empty()
while True:
    try:
        orders = load_orders()
        carts = load_cart_events()
        pages = load_page_views()

        with placeholder.container():
            c1, c2 = st.columns(2)
            c1.subheader("Ventas por minuto")
            c1.line_chart(orders, x="minute_str", y="total_price")

            c2.subheader("Actividad carrito (categoría/tipo)")
            c2.bar_chart(carts, x="category", y="count", color="event_type")

            st.subheader("Productos más visitados (top 20)")
            st.bar_chart(pages, x="product_label", y="count")

        time.sleep(REFRESH_EVERY)
    except Exception as e:
        st.error(f"Error leyendo HDFS: {e}")
        time.sleep(REFRESH_EVERY)