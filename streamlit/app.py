import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="CSV Explorer", layout="wide")
st.title("CSV Explorer")

# Option A: upload a CSV (best for demos)
uploaded = st.file_uploader("Upload a CSV", type=["csv"])

if uploaded is None:
    st.info("Upload a CSV to get started.")
    st.stop()

@st.cache_data
def load_csv(file) -> pd.DataFrame:
    return pd.read_csv(file)

df = load_csv(uploaded)

st.subheader("Data")
st.dataframe(df, use_container_width=True)

# --- Something “fancy”: interactive filtering + chart ---
st.subheader("Explore")
cols = df.columns.tolist()

with st.sidebar:
    st.header("Controls")
    numeric_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols = [c for c in cols if df[c].dtype == "object" or str(df[c].dtype).startswith("category")]

    x = st.selectbox("X axis", options=numeric_cols or cols, index=0)
    y = st.selectbox("Y axis", options=numeric_cols or cols, index=min(1, len((numeric_cols or cols))-1))
    color = st.selectbox("Color (optional)", options=["(none)"] + cat_cols)

    if cat_cols:
        filter_col = st.selectbox("Filter column", options=["(none)"] + cat_cols)
    else:
        filter_col = "(none)"

df_view = df.copy()
if 'filter_col' in locals() and filter_col != "(none)":
    choices = st.multiselect(f"Keep values in {filter_col}", sorted(df_view[filter_col].dropna().unique().tolist()))
    if choices:
        df_view = df_view[df_view[filter_col].isin(choices)]

st.caption(f"Rows shown: {len(df_view):,}")

fig = px.scatter(
    df_view,
    x=x,
    y=y,
    color=None if color == "(none)" else color,
    hover_data=cols[:10],  # keep hover sane
)
st.plotly_chart(fig, use_container_width=True)

st.download_button(
    "Download filtered CSV",
    data=df_view.to_csv(index=False).encode("utf-8"),
    file_name="filtered.csv",
    mime="text/csv",
)