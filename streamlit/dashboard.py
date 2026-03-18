import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px

# ── Page Config ───────────────────────────────────────────
st.set_page_config(
    page_title="Instacart Analytics Dashboard",
    page_icon="🛒",
    layout="wide"
)

# ── Snowflake Connection ───────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account   = st.secrets["snowflake"]["account"],
        user      = st.secrets["snowflake"]["user"],
        password  = st.secrets["snowflake"]["password"],
        role      = st.secrets["snowflake"]["role"],
        warehouse = st.secrets["snowflake"]["warehouse"],
        database  = st.secrets["snowflake"]["database"],
    )

@st.cache_data(ttl=3600)
def run_query(query):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(query)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    df   = pd.DataFrame(rows, columns=cols)
    df.columns = [c.upper() for c in df.columns]
    return df

# ── Header ─────────────────────────────────────────────────
st.title("🛒 Instacart Analytics Dashboard")
st.subheader("by Duncan Otieno")
st.markdown("Production data from Snowflake · Pipeline: dbt · Airflow · GitHub Actions · Streamlit")
st.divider()

# ── KPI Metrics ────────────────────────────────────────────
st.subheader("Key Metrics")

kpi = run_query("""
    SELECT
        COUNT(DISTINCT order_id)            AS total_orders,
        COUNT(DISTINCT user_key)            AS total_users,
        ROUND(AVG(total_items), 1)          AS avg_basket_size,
        ROUND(AVG(order_reorder_rate), 3)   AS avg_reorder_rate
    FROM TRANSFORM_DB.DBT_PROD_MARTS_CORE.FACT_ORDERS
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders",     f"{int(kpi['TOTAL_ORDERS'][0]):,}")
col2.metric("Total Customers",  f"{int(kpi['TOTAL_USERS'][0]):,}")
col3.metric("Avg Basket Size",  f"{kpi['AVG_BASKET_SIZE'][0]}")
col4.metric("Avg Reorder Rate", f"{round(float(kpi['AVG_REORDER_RATE'][0])*100, 1)}%")

st.divider()

# ── Row 1: Customer Segments + Loyalty Tiers ───────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Customer Segments")
    st.caption("Number of customers per RFM segment")
    segments = run_query("""
        SELECT customer_segment, COUNT(*) AS customers,
               ROUND(AVG(reorder_rate)*100, 1) AS avg_reorder_pct
        FROM TRANSFORM_DB.DBT_PROD_MARTS_MARKETING.MART_CUSTOMER_360
        GROUP BY customer_segment
        ORDER BY customers DESC
    """)
    fig = px.bar(
        segments,
        x="CUSTOMERS",
        y="CUSTOMER_SEGMENT",
        orientation="h",
        color="AVG_REORDER_PCT",
        color_continuous_scale="Teal",
        text="CUSTOMERS",
        labels={
            "CUSTOMERS":        "Number of Customers",
            "CUSTOMER_SEGMENT": "Segment",
            "AVG_REORDER_PCT":  "Avg Reorder %"
        }
    )
    fig.update_traces(texttemplate='%{text:,}', textposition='outside')
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        coloraxis_colorbar_title="Reorder %",
        height=400,
        margin=dict(l=10, r=80, t=10, b=10)
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Loyalty Tiers")
    st.caption("Customer count and average reorder rate per tier")
    loyalty = run_query("""
        SELECT loyalty_tier, COUNT(*) AS customers,
               ROUND(AVG(reorder_rate)*100, 1) AS avg_reorder_pct
        FROM TRANSFORM_DB.DBT_PROD_MARTS_MARKETING.MART_CUSTOMER_360
        GROUP BY loyalty_tier
        ORDER BY customers DESC
    """)
    fig = px.bar(
        loyalty,
        x="LOYALTY_TIER",
        y="CUSTOMERS",
        color="AVG_REORDER_PCT",
        color_continuous_scale="Blues",
        text="CUSTOMERS",
        labels={
            "CUSTOMERS":       "Number of Customers",
            "LOYALTY_TIER":    "Loyalty Tier",
            "AVG_REORDER_PCT": "Avg Reorder %"
        }
    )
    fig.update_traces(texttemplate='%{text:,}', textposition='outside')
    fig.update_layout(
        height=400,
        margin=dict(l=10, r=10, t=10, b=10)
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 2: Top Products + Orders by Hour ───────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 15 Products by Reorder Rate")
    st.caption("Products ordered 1000+ times — higher % means customers keep coming back for it")
    products = run_query("""
        SELECT
            product_name,
            department_name,
            ROUND(reorder_rate * 100, 1) AS reorder_pct,
            times_ordered
        FROM TRANSFORM_DB.DBT_PROD_MARTS_PRODUCT.MART_PRODUCT_PERFORMANCE
        WHERE times_ordered > 1000
        ORDER BY reorder_rate DESC
        LIMIT 15
    """)
    if not products.empty:
        fig = px.bar(
            products,
            x="REORDER_PCT",
            y="PRODUCT_NAME",
            orientation="h",
            color="DEPARTMENT_NAME",
            text="REORDER_PCT",
            labels={
                "REORDER_PCT":     "Reorder Rate %",
                "PRODUCT_NAME":    "Product",
                "DEPARTMENT_NAME": "Department"
            },
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig.update_traces(texttemplate='%{text}%', textposition='outside')
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            xaxis_range=[0, 110],
            height=500,
            margin=dict(l=10, r=60, t=10, b=10),
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Peak Ordering Hours")
    st.caption("Total orders placed each hour across all days")
    hourly = run_query("""
        SELECT
            order_hour_of_day AS hour,
            COUNT(*)          AS order_count
        FROM TRANSFORM_DB.DBT_PROD_MARTS_CORE.FACT_ORDERS
        GROUP BY order_hour_of_day
        ORDER BY order_hour_of_day
    """)
    if not hourly.empty:
        hourly['HOUR_LABEL'] = hourly['HOUR'].astype(str) + ':00'
        fig = px.area(
            hourly,
            x="HOUR_LABEL",
            y="ORDER_COUNT",
            labels={
                "HOUR_LABEL":   "Hour of Day",
                "ORDER_COUNT":  "Total Orders"
            },
            color_discrete_sequence=["#2E86C1"],
            line_shape="spline"
        )
        fig.update_traces(fill='tozeroy', fillcolor='rgba(46,134,193,0.2)')
        fig.update_layout(
            height=500,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 3: Orders by Day of Week ──────────────────────────
st.subheader("Orders by Day of Week")
st.caption("Which days do customers order most?")

daily = run_query("""
    SELECT
        order_day_name  AS day,
        order_day_of_week AS day_num,
        COUNT(*)        AS order_count
    FROM TRANSFORM_DB.DBT_PROD_MARTS_CORE.FACT_ORDERS
    GROUP BY order_day_name, order_day_of_week
    ORDER BY order_day_of_week
""")

if not daily.empty:
    fig = px.bar(
        daily,
        x="DAY",
        y="ORDER_COUNT",
        color="ORDER_COUNT",
        color_continuous_scale="Blues",
        text="ORDER_COUNT",
        labels={
            "DAY":         "Day of Week",
            "ORDER_COUNT": "Total Orders"
        }
    )
    fig.update_traces(texttemplate='%{text:,}', textposition='outside')
    fig.update_layout(
        height=400,
        showlegend=False,
        margin=dict(l=10, r=10, t=10, b=10),
        coloraxis_showscale=False
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 4: Department Performance ─────────────────────────
st.subheader("Department Performance")
st.caption("Bubble size = unique orders · Y axis = reorder rate · X axis = total items ordered")

depts = run_query("""
    SELECT
        p.department_name,
        COUNT(*)                                     AS total_line_items,
        ROUND(AVG(fop.is_reordered::INT) * 100, 1)  AS reorder_pct,
        COUNT(DISTINCT fop.order_id)                 AS unique_orders
    FROM TRANSFORM_DB.DBT_PROD_MARTS_CORE.FACT_ORDER_PRODUCTS fop
    JOIN TRANSFORM_DB.DBT_PROD_MARTS_CORE.DIM_PRODUCTS p
      ON fop.product_key = p.product_key
    WHERE p.is_current = TRUE
    GROUP BY p.department_name
    ORDER BY total_line_items DESC
""")

if not depts.empty:
    fig = px.scatter(
        depts,
        x="TOTAL_LINE_ITEMS",
        y="REORDER_PCT",
        size="UNIQUE_ORDERS",
        color="DEPARTMENT_NAME",
        hover_name="DEPARTMENT_NAME",
        text="DEPARTMENT_NAME",
        labels={
            "TOTAL_LINE_ITEMS": "Total Items Ordered",
            "REORDER_PCT":      "Reorder Rate %",
            "DEPARTMENT_NAME":  "Department"
        },
        size_max=60
    )
    fig.update_traces(textposition='top center')
    fig.update_layout(
        height=500,
        showlegend=False,
        margin=dict(l=10, r=10, t=10, b=10)
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 5: Order Frequency Distribution ───────────────────
st.subheader("Customer Order Frequency")
st.caption("How often do customers place orders?")

frequency = run_query("""
    SELECT order_frequency_bucket,
           COUNT(*) AS customers,
           ROUND(AVG(reorder_rate)*100, 1) AS avg_reorder_pct
    FROM TRANSFORM_DB.DBT_PROD_MARTS_MARKETING.MART_CUSTOMER_360
    GROUP BY order_frequency_bucket
    ORDER BY customers DESC
""")

if not frequency.empty:
    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.bar(
            frequency,
            x="ORDER_FREQUENCY_BUCKET",
            y="CUSTOMERS",
            color="AVG_REORDER_PCT",
            color_continuous_scale="Teal",
            text="CUSTOMERS",
            labels={
                "ORDER_FREQUENCY_BUCKET": "Order Frequency",
                "CUSTOMERS":              "Number of Customers",
                "AVG_REORDER_PCT":        "Avg Reorder %"
            }
        )
        fig.update_traces(texttemplate='%{text:,}', textposition='outside')
        fig.update_layout(
            height=350,
            margin=dict(l=10, r=10, t=10, b=10)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(
            frequency.rename(columns={
                "ORDER_FREQUENCY_BUCKET": "Frequency",
                "CUSTOMERS":              "Customers",
                "AVG_REORDER_PCT":        "Avg Reorder %"
            }),
            hide_index=True,
            use_container_width=True
        )

st.caption(
    "Built by Duncan Otieno · Data source: Snowflake TRANSFORM_DB · "
    "Pipeline: dbt + Airflow + GitHub Actions · "
    "Repo: github.com/Duncan610/instacart-analytics-pipeline"
)
