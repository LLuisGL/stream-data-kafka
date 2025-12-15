import streamlit as st
import pandas as pd
import plotly.express as px
import pyarrow.parquet as pq
import time

st.set_page_config(page_title="E-commerce Realtime Dashboard", layout="wide")

REFRESH_INTERVAL = 30  # segundos

# -----------------------------
# Helpers
# -----------------------------
def load_parquet(path):
    table = pq.read_table(path)
    return table.to_pandas()

# -----------------------------
# Auto-refresh
# -----------------------------

st.title("📊 Dashboard de E-commerce (Tiempo Casi Real)")

st.caption("Datos procesados con Spark Structured Streaming")

# -----------------------------
# Ventas por minuto
# -----------------------------
st.subheader("💰 Ventas por Minuto")

sales_df = load_parquet("data/sales_per_minute")

if not sales_df.empty:
    fig_sales = px.line(
        sales_df,
        x="minute",
        y="total_sales",
        title="Ventas por Minuto"
    )
    st.plotly_chart(fig_sales, use_container_width=True)
else:
    st.info("Esperando datos de ventas...")

# -----------------------------
# Actividad por categoría
# -----------------------------
st.subheader("📦 Actividad por Categoría")

cat_df = load_parquet("data/categories")

if not cat_df.empty:
    latest_window = cat_df["window_start"].max()
    latest = cat_df[cat_df["window_start"] == latest_window]

    fig_cat = px.bar(
        latest,
        x="category",
        y=["total_views", "total_carts", "total_purchases"],
        title="Actividad por Categoría (última ventana)",
        barmode="group"
    )
    st.plotly_chart(fig_cat, use_container_width=True)

# -----------------------------
# Eventos de carrito
# -----------------------------
st.subheader("🛒 Eventos del Carrito")

users_df = load_parquet("data/users")

if not users_df.empty:
    fig_cart = px.scatter(
        users_df,
        x="total_views",
        y="total_purchases",
        size="total_carts",
        hover_name="user_id",
        title="Comportamiento de Usuarios"
    )
    st.plotly_chart(fig_cart, use_container_width=True)

# -----------------------------
# Productos más visitados
# -----------------------------
st.subheader("🔥 Productos Más Visitados")

prod_df = load_parquet("data/products")

if not prod_df.empty:
    top_products = (
        prod_df.groupby(["product_id", "category"])["total_views"]
        .sum()
        .reset_index()
        .sort_values("total_views", ascending=False)
        .head(10)
    )

    fig_prod = px.bar(
        top_products,
        x="product_id",
        y="total_views",
        color="category",
        title="Top 10 Productos Más Visitados"
    )
    st.plotly_chart(fig_prod, use_container_width=True)

# -----------------------------
# Auto refresh
# -----------------------------
time.sleep(REFRESH_INTERVAL)
st.rerun()

