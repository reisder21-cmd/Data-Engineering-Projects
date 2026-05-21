# Streamlit dashboarding for Global Business Partners Project

# Getting CLV by gathering:
# 1. Revenue per order
# 2. total spend per customer_id

# Grouping CLV values:
 # --High Top 20%
# -- Medium Mid 60%
# -- Low Bottom 20%

from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

DATA_DIR = Path(__file__).parent / "data"

#--------------------------------
# Page Setup
# -------------------------------

st.set_page_config(
    page_title="Global Business Partners Insights",
    page_icon=":restaurant:",
    layout="wide"
)

#-------------------------------------------------
# Cache Data for faster re-renders
# ------------------------------------------------

@st.cache_data
def load_data():
    initial_gold = pd.read_parquet(DATA_DIR / "fact_order_items.parquet")
    customer_summary = pd.read_parquet(DATA_DIR / "customer_summary.parquet")
    return initial_gold, customer_summary

try:
    initial_gold_df, customer_summary_df = load_data()
except FileNotFoundError as e:
    st.error(
        f"Missing local data files, Run 'gpb_data_refresh.py first. \n\n"
        f"Details {e}"
    )
    st.stop()

# ------------------------
# Header
#------------------------

st.title("Global Partners Business Insights Dashboard")
st.caption(
    f"Caption here if needed"
)

# Aggregate data by user
customer_summary_df = customer_summary_df[customer_summary_df['user_id'].notnull()]
user_revenue = customer_summary_df.groupby('user_id', as_index=False)['total_revenue'].sum()

top_10_users = user_revenue.sort_values(by='total_revenue', ascending=False).head(10)
top_10_users.reset_index(drop=True, inplace=True)
#top_10_users.index= top_10_users.index +1

col1,col2,col3 = st.columns(3)

total_rev = user_revenue['total_revenue'].sum()
top_10_rev = top_10_users['total_revenue'].sum()
#avg_revenue = user_revenue['total_revenue'].mean()

col1.metric(label="total Revenue", value=f"${total_rev:,.2f}")
col2.metric(label= "Top User Revenue", value=f"{top_10_users['total_revenue'].max():,.2f}")
col3.metric(label="Average Revenue Overall Per Customer", value=f"${customer_summary_df['total_revenue'].mean():,.2f}")

st.subheader("Top 10 Users")
st.dataframe(top_10_users, use_container_width=True)

st.divider()

#-------------------------------------------------

st.subheader("Customer Lifetime Value Tiers")

clv_tiers_customers = customer_summary_df.groupby("clv_tier").agg(
    Total_customers = ("user_id", "count")
).reset_index()

#st.write("Aggregated Data:", clv_tiers_customers)

fig = px.pie(clv_tiers_customers, values = 'Total_customers', names = 'clv_tier')

st.plotly_chart(fig)

