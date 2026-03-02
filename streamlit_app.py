# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import plotly.express as px

# Get the current credentials
session = get_active_session()

with open("brazil_geo.json", "r") as f:
    brazil_states = json.load(f)

# --------------------------------------------------
# Calculate metrics

df_sales_by_year = session.sql(
    """
    SELECT
        COUNT(DISTINCT 
                CASE 
                    WHEN DATE_TRUNC('month',order_purchase_date) 
                    BETWEEN '2017-01-01' AND '2017-08-01' THEN order_id
                END) AS "total_orders_2017",
        SUM(CASE 
                WHEN DATE_TRUNC('month',order_purchase_date) 
                BETWEEN '2017-01-01' AND '2017-08-01' THEN price
                ELSE 0
            END) AS "total_sales_2017",
        COUNT(DISTINCT 
                CASE 
                    WHEN DATE_TRUNC('month',order_purchase_date) 
                    BETWEEN '2018-01-01' AND '2018-08-01' THEN order_id
                END) AS "total_orders_2018",
        SUM(CASE 
                WHEN DATE_TRUNC('month',order_purchase_date) 
                BETWEEN '2018-01-01' AND '2018-08-01' THEN price
                ELSE 0
            END) AS "total_sales_2018"
    FROM fact_sales  
"""
).to_pandas()

# st.write(df_sales_by_year)
st.subheader("KPIs for YTD 2018 Jan-Aug")
sales_2017 = df_sales_by_year["total_sales_2017"][0]
order_count_2017 = df_sales_by_year["total_orders_2017"][0]
avg_2017 = sales_2017 / order_count_2017

sales_2018 = df_sales_by_year["total_sales_2018"][0]
order_count_2018 = df_sales_by_year["total_orders_2018"][0]
avg_2018 = sales_2018 / order_count_2018

col1, col2, col3 = st.columns(3)
col1.metric(
    label="Total Revenue in Million",
    value=f"R${sales_2018/1000000:,.2f}",
    delta=f"{(sales_2018 - sales_2017)/sales_2017:.2%}",
)
col2.metric(
    label="Total Orders",
    value=f"{order_count_2018:,}",
    delta=f"{(order_count_2018 - order_count_2017)/order_count_2017:.2%}",
)
col3.metric(
    label="Avg Sales per Order",
    value=f"R${avg_2018:,.2f}",
    delta=f"{(avg_2018 - avg_2017)/avg_2017:.2%}",
)


# --------------------------------------------------
# Map plot by total sales
df_sales = session.sql(
    """
    WITH sales_cte AS (
        SELECT 
            d.customer_state,
            f.price
        FROM fact_sales f
        LEFT JOIN dim_customers d
        ON f.customer_id = d.customer_id
        )

    SELECT 
        customer_state AS "customer_state", 
        SUM(price) AS "total_sales_by_state"
    FROM sales_cte GROUP BY customer_state
    ORDER BY "total_sales_by_state" DESC
"""
).to_pandas()

map_fig = px.choropleth_mapbox(
    df_sales,
    geojson=brazil_states,
    locations="customer_state",
    featureidkey="id",
    color="total_sales_by_state",
    title="Total Sales by State in Brazil (2016/09 - 2018/09)",
    mapbox_style="carto-darkmatter",
    zoom=2.5,
    center={"lat": -15.793889, "lon": -47.882778},
    color_continuous_scale=[
        [0.0, "#E0F3F8"],  # 0% of max: Very light blue
        [0.05, "#74ADD1"],  # 5% of max (~200k): Medium light blue
        [0.25, "#4575B4"],  # 25% of max (~1M): Medium dark blue
        [1.0, "#313695"],  # 100% of max (~4M): Deepest dark blue
    ],
)
map_fig.update_layout(title_x=0.05)

st.plotly_chart(map_fig)
# --------------------------------------------------

# Trend line graph by sales

df_sales_trend = session.sql(
    """
    SELECT
        DATE_TRUNC('month',order_purchase_date) AS "sales_year_month",
        SUM(price) AS "total_sales"
    FROM fact_sales
    WHERE EXTRACT(year from order_purchase_date) > 2016 
        AND DATE_TRUNC('month',order_purchase_date) < '2018-09-01'
    GROUP BY DATE_TRUNC('month',order_purchase_date)
    ORDER BY "sales_year_month"   
"""
).to_pandas()

trend_fig = px.line(
    df_sales_trend,
    x="sales_year_month",
    y="total_sales",
    title="Total Sales by Year-Month",
)

trend_fig.update_layout(title_x=0.35, title={"font": {"size": 12}})

# --------------------------------------------------

# Bar chart of top 10 best selling product categories

df_sales_by_prod_cat = session.sql(
    """
    SELECT
        d.product_category_name AS "product_category_name",
        SUM(f.price) AS "total_sales"
    FROM fact_sales f
    LEFT JOIN dim_products d
    ON f.product_id = d.product_id
    GROUP BY d.product_category_name
    ORDER BY "total_sales" DESC
    LIMIT 10
"""
).to_pandas()

bar_fig = px.bar(
    df_sales_by_prod_cat,
    x="product_category_name",
    y="total_sales",
    title="Top 10 Best Selling Product Categories (2016/09 - 2018/09)",
    color_discrete_sequence=["mediumpurple"],
)
bar_fig.update_layout(title_x=0.15, title={"font": {"size": 10}})


# --------------------------------------------------
# Bar chart for sales by day of week
df_sales_by_day_of_week = session.sql(
    """
    SELECT
        d.day_of_week AS "day_of_week",
        SUM(f.price) AS "total_price"
    FROM fact_sales f
    LEFT JOIN dim_dates d
    ON f.order_purchase_date = d.date_day
    GROUP BY d.day_of_week
"""
).to_pandas()

day_of_week_bar = px.bar(
    df_sales_by_day_of_week,
    x="day_of_week",
    y="total_price",
    title="Sales by Day of the Week",
    category_orders={
        "day_of_week": [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
    },
)
day_of_week_bar.update_layout(title_x=0.35, title={"font": {"size": 12}})


# --------------------------------------------------
# Box-whisker plot to analyze Monday which has the highest sales

df_sales_by_day_of_week = session.sql(
    """
    SELECT
        d.date_day AS "date_day",
        d.day_of_week AS "day_of_week",
        SUM(f.price) AS "total_price"
    FROM fact_sales f
    INNER JOIN dim_dates d
    ON f.order_purchase_date = d.date_day
    GROUP BY d.date_day, d.day_of_week
"""
).to_pandas()

box_fig = px.box(
    df_sales_by_day_of_week,
    x="day_of_week",
    y="total_price",
    title="Distribution of Sales by Day of the Week",
    color="day_of_week",
    category_orders={
        "day_of_week": [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
    },
)
box_fig.update_layout(title_x=0.2)
st.plotly_chart(box_fig)


# --------------------------------------------------
# Donut chart for total payment by payment method

df_sales_by_payment_method = session.sql(
    """
    SELECT
        payment_type AS "payment_type",
        SUM(payment_value) AS "total_value"
    FROM fact_payments
    GROUP BY payment_type
    HAVING SUM(payment_value) > 0
"""
).to_pandas()

donut_fig = px.pie(
    df_sales_by_payment_method,
    names="payment_type",
    values="total_value",
    title="Total Payment Amount by Payment Type",
    hole=0.4,
    color_discrete_sequence=px.colors.qualitative.Vivid,
)
donut_fig.update_layout(title_x=0.15, margin=dict(l=80), title={"font": {"size": 12}})


# --------------------------------------------------

col1, col2 = st.columns([0.45, 0.55])
with col1:
    st.plotly_chart(trend_fig, use_container_width=True)
    st.plotly_chart(day_of_week_bar, use_container_width=True)
with col2:
    st.plotly_chart(bar_fig, use_container_width=True)
    st.plotly_chart(donut_fig, use_container_width=True)

# --------------------------------------------------
# Funnel chart of customer count by number of installments
df_customer_cnt_by_installments = session.sql(
    """
    SELECT 
        p.payment_installments AS "payment_installments",
        COUNT(DISTINCT c.customer_unique_id) AS "customer_cnt"
    FROM fact_payments p
    LEFT JOIN dim_customers c
    ON p.customer_id = c.customer_id
    WHERE p.payment_installments > 0
    GROUP BY p.payment_installments
    ORDER BY p.payment_installments
"""
).to_pandas()
df_customer_cnt_by_installments["installment_label"] = (
    df_customer_cnt_by_installments["payment_installments"].astype(str) + "x"
)

installment_bar = px.bar(
    df_customer_cnt_by_installments,
    x="installment_label",
    y="customer_cnt",
    title="Customer Count by Installment Type",
    color="customer_cnt",
    color_continuous_scale="peach",
)
installment_bar.update_layout(
    title_x=0.36, title={"font": {"size": 14}}, coloraxis_showscale=False
)
st.plotly_chart(installment_bar)
