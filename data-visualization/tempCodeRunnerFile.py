import time
import pandas as pd
import streamlit as st
import pyarrow.dataset as ds
import pyarrow.fs as pafs

HDFS_URI = "hdfs://localhost:9000"
BASE = "/user/luis-goncalves/ecommerce"
REFRESH_EVERY = 10  # segundos
NUM_PARTITIONS = 1  # cuántas fechas cargar (1 = solo la más reciente)

fs = pafs.HadoopFileSystem.from_uri(HDFS_URI)

def latest_partitions(path, num=1):
    dataset = ds.dataset(f"{path}", filesystem=fs, format="parquet")
    parts = sorted({frag.partition_expression for frag in dataset.get_fragments()},
                   key=lambda e: str(e), reverse=True)
    return parts[:num] if parts else []

def read_parquet(path, partitions=None):
    dataset = ds.dataset(f"{path}", filesystem=fs, format="parquet")
    if partitions:
        filt = ds.field("fecha").isin([p.to_pydict()["fecha"][0] for p in partitions])
        dataset = ds.dataset(f"{path}", filesystem=fs, format="parquet", partitioning="hive").replace_schema_metadata({}).filter(filt)
    return dataset.to_table().to_pandas()

def load_orders():
    parts = latest_partitions(f"{BASE}/orders", NUM_PARTITIONS)
    df = read_parquet(f"{BASE}/orders", partitions=parts)
    # si tu ventana viene como struct window.start/end
    if "window" in df.columns:
        df["minute"] = df["window"].apply(lambda w: w["start"])
    elif "event_time" in df.columns:
        df["minute"] = df["event_time"]
    return (df.groupby("minute", as_index=False)["total_price"].sum()
              .sort_values("minute"))

def load_cart_events():
    parts = latest_partitions(f"{BASE}/cart_events", NUM_PARTITIONS)
    df = read_parquet(f"{BASE}/cart_events", partitions=parts)
    return (df.groupby(["category", "event_type"], as_index=False)["count"].sum()
              .sort_values("count", ascending=False))

def load_page_views():
    parts = latest_partitions(f"{BASE}/page_views", NUM_PARTITIONS)
    df = read_parquet(f"{BASE}/page_views", partitions=parts)
    return (df.groupby(["product_id", "category"], as_index=False)["count"].sum()
              .sort_values("count", ascending=False)
              .head(20))

st.set_page_config(page_title="Ecommerce Streaming", layout="wide")
st.title("Dash en vivo desde HDFS (particionado por fecha)")

placeholder = st.empty()

while True:
    try:
        orders = load_orders()
        carts = load_cart_events()
        pages = load_page_views()

        with placeholder.container():
            c1, c2 = st.columns(2)
            c1.subheader("Ventas por minuto")
            c1.line_chart(orders, x="minute", y="total_price")

            c2.subheader("Actividad carrito (por categoría y tipo)")
            c2.bar_chart(carts, x="category", y="count", color="event_type")

            st.subheader("Productos más visitados (top 20)")
            st.bar_chart(pages, x="product_id", y="count")

        time.sleep(REFRESH_EVERY)
        st.experimental_rerun()
    except Exception as e:
        st.error(f"Error leyendo HDFS: {e}")
        time.sleep(REFRESH_EVERY)