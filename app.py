import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Multi-Channel Inventory Hub", layout="wide")

# --- UTILITY FUNCTIONS ---

def clean_percentage(val):
    if isinstance(val, str):
        return float(val.replace('%', '').replace(',', '')) / 100
    return val

def clean_currency(val):
    if isinstance(val, str):
        return float(val.replace('₹', '').replace(',', ''))
    return val

# --- CHANNEL PARSERS ---

def parse_amazon(df):
    # Detection: 'ASIN'
    df = df.copy()
    df['Sell-Through %'] = df['Sell-Through %'].apply(clean_percentage)
    df['Sellable On Hand Units'] = pd.to_numeric(df['Sellable On Hand Units'], errors='coerce').fillna(0)
    return df.rename(columns={
        'ASIN': 'channel_sku',
        'Sellable On Hand Units': 'inventory',
        'Sell-Through %': 'str'
    })[['channel_sku', 'inventory', 'str']]

def parse_blinkit(inv_df, sales_df=None):
    # Detection: 'Item ID', 'Total sellable'
    inv_df['DRR'] = pd.to_numeric(inv_df['Last 30 days'], errors='coerce').fillna(0) / 30
    inv_df['inventory'] = pd.to_numeric(inv_df['Total sellable'], errors='coerce').fillna(0)
    inv_df['doc'] = inv_df['inventory'] / inv_df['DRR'].replace(0, 1)
    return inv_df.rename(columns={'Item ID': 'channel_sku', 'Warehouse Facility Name': 'warehouse'})

def parse_swiggy(inv_df, sales_df):
    # Detection: 'SkuCode' (Inv), 'ITEM_CODE' (Sales)
    # Calculate DRR from sales
    drr = sales_df.groupby('ITEM_CODE')['UNITS_SOLD'].sum() / 30
    inv_df = inv_df.merge(drr, left_on='SkuCode', right_index=True, how='left').fillna(0)
    inv_df['inventory'] = inv_df['WarehouseQtyAvailable']
    inv_df['doc'] = inv_df['inventory'] / inv_df['UNITS_SOLD'].replace(0, 1)
    return inv_df.rename(columns={'SkuCode': 'channel_sku', 'FacilityName': 'warehouse'})

def parse_bigbasket(inv_df):
    # Detection: 'SKU_Id', 'Total SOH'
    inv_df['inventory'] = pd.to_numeric(inv_df['Total SOH'], errors='coerce').fillna(0)
    inv_df['doc'] = pd.to_numeric(inv_df['SOH Day of Cover (HO)'], errors='coerce').fillna(0)
    return inv_df.rename(columns={'SKU_Id': 'channel_sku', 'DC': 'warehouse'})

# --- MAIN APP ---

st.title("📦 Inventory Health & Sell-Through Dashboard")

# 1. MAPPING CONFIGURATION
with st.expander("⚙️ Step 1: Upload Product Mapping Config"):
    st.info("Upload a CSV with columns: [channel, channel_sku, master_sku]")
    mapping_file = st.file_uploader("Upload Mapping CSV", type=['csv'])
    if mapping_file:
        mapping_df = pd.read_csv(mapping_file)
        st.success("Mapping Loaded")
    else:
        st.warning("Please upload a mapping file to reconcile different channel SKUs.")
        mapping_df = pd.DataFrame(columns=['channel', 'channel_sku', 'master_sku'])

# 2. DATA UPLOAD
st.subheader("📥 Step 2: Upload Channel Data")
cols = st.columns(4)

with cols[0]:
    st.image("https://upload.wikimedia.org/wikipedia/commons/a/a9/Amazon_logo.svg", width=80)
    amz_file = st.file_uploader("Amazon Inv", type=['csv'])

with cols[1]:
    st.write("**Blinkit**")
    blk_inv = st.file_uploader("Blinkit Inv", type=['csv'])
    blk_sls = st.file_uploader("Blinkit Sales", type=['csv'])

with cols[2]:
    st.write("**Swiggy**")
    swg_inv = st.file_uploader("Swiggy Inv", type=['csv'])
    swg_sls = st.file_uploader("Swiggy Sales", type=['csv'])

with cols[3]:
    st.write("**Big Basket**")
    bb_inv = st.file_uploader("BB Inv", type=['csv'])

# 3. PROCESSING LOGIC
processed_data = []

if amz_file:
    df = pd.read_csv(amz_file, skiprows=1)
    clean_df = parse_amazon(df)
    clean_df['channel'] = 'Amazon'
    processed_data.append(clean_df)

if blk_inv:
    df = pd.read_csv(blk_inv, skiprows=2)
    clean_df = parse_blinkit(df)
    clean_df['channel'] = 'Blinkit'
    processed_data.append(clean_df)

if swg_inv and swg_sls:
    i_df = pd.read_csv(swg_inv)
    s_df = pd.read_csv(swg_sls)
    clean_df = parse_swiggy(i_df, s_df)
    clean_df['channel'] = 'Swiggy'
    processed_data.append(clean_df)

if bb_inv:
    df = pd.read_csv(bb_inv)
    clean_df = parse_bigbasket(df)
    clean_df['channel'] = 'Big Basket'
    processed_data.append(clean_df)

if processed_data:
    master_df = pd.concat(processed_data, ignore_index=True)
    
    # Apply Mapping
    if not mapping_df.empty:
        master_df = master_df.merge(mapping_df, on=['channel', 'channel_sku'], how='left')
        master_df['master_sku'] = master_df['master_sku'].fillna("UNMAPPED")
    
    # Calculate Days of Cover for all
    if 'doc' not in master_df.columns:
        master_df['doc'] = 0 # Amazon logic usually provides STR directly
    
    # 4. DASHBOARD VISUALS
    st.divider()
    
    # Key Alerts
    low_stock = master_df[master_df['doc'] < 15]
    st.error(f"🚨 {len(low_stock)} items are UNDERSTOCKED (< 15 days cover)")
    
    # Trend Lines
    st.subheader("📈 Inventory Trend Analysis")
    selected_sku = st.selectbox("Select Master SKU", master_df['master_sku'].unique())
    
    sku_view = master_df[master_df['master_sku'] == selected_sku]
    
    fig = px.bar(sku_view, x='channel', y='inventory', color='warehouse', 
                 title=f"Inventory Distribution for {selected_sku}",
                 labels={'inventory': 'Stock Units', 'channel': 'Channel'})
    st.plotly_chart(fig, use_container_width=True)

    # Inventory Table
    st.subheader("📋 Master Inventory View")
    
    def color_risk(val):
        if val < 15: return 'background-color: #ffcccc'
        if val > 60: return 'background-color: #fff3cd'
        return ''

    st.dataframe(master_df[['master_sku', 'channel', 'warehouse', 'inventory', 'doc', 'str']].style.applymap(color_risk, subset=['doc']))
else:
    st.info("Awaiting file uploads to generate dashboard.")
