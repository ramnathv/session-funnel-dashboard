import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
from utils import (
    enrich_data,
    sankify,
    visualize_funnel_sankey,
    visualize_step_matrix,
    plot_funnel,
)

pd.options.plotting.backend = "plotly"


import streamlit as st

st.set_page_config(layout="wide")

st.title("Session Conversion Dashboard")

data = pd.read_csv("data/events.csv")
data_enriched = enrich_data(data)

option = st.sidebar.selectbox(
    "How would you like to visualize session flow?",
    ("Funnel", "Sankey Unordered", "Sankey Ordered", "Step Matrix"),
)


if option == "Funnel":
    targets = ["page_view", "add_to_cart", "checkout"]
    fig = plot_funnel(data, targets)
elif option == "Sankey Unordered":
    fig = visualize_funnel_sankey(data_enriched)
elif option == "Sankey Ordered":
    fig = visualize_funnel_sankey(data_enriched, use_step=True)
else:
    fig = visualize_step_matrix(data_enriched)


st.markdown(f"#### {option}")
st.plotly_chart(fig, use_container_width=True)
