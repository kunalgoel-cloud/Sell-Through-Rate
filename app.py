import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- DATABASE & CONFIG ---
st.set_page_config(page_title="Mama Nourish | Cloud Inventory Hub", layout="wide")

try:
    conn_url = st.secrets["connections"]["postgresql"]["url"]
    engine = create_engine(conn_url)
except Exception as e:
    st.error("Neon Connection string not found in secrets.")
    st.stop()

COMMON_SKU_FILE = "2026-03-28T16-00_export.csv"

# --- DB FUNCTIONS ---

def init_db():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sku_mappings (
                id SERIAL PRIMARY KEY,
                channel TEXT NOT NULL,
                channel_sku TEXT NOT NULL,
                master_sku TEXT NOT NULL,
                UNIQUE(channel, channel_sku)
            );
        """))
        conn.commit()

def load_mapping_from_db():
    query = "SELECT channel, channel_sku, master_sku FROM sku_mappings"
    return pd.read_sql(query, engine).astype(str)

def save_mapping_to_db(new_entries):
    with engine.connect() as conn:
        for entry in new_entries:
            conn.execute(
                text("""
                    INSERT INTO sku_mappings (channel, channel_sku, master_sku)
                    VALUES (:channel, :channel_sku, :master_sku)
                    ON CONFLICT (channel, channel_sku) 
                    DO UPDATE SET master_sku = EXCLUDED.master_sku
                """),
                entry
            )
        conn.commit()

# --- CHANNEL PARSERS ---

def calculate_str(sold, inv):
    total = sold + inv
    return sold / total if total > 0 else 0

def parse_amazon(df):
    df = df.copy()
    df['channel_sku'] = df['ASIN'].astype(str).str.strip()
    df['inventory'] = pd.to_numeric(df['Sellable On Hand Units'], errors='coerce').fillna(0)
    # Amazon report provides STR directly as a percentage string
    df['str'] = pd.to_numeric(df['Sell-Through %'].astype(str).str.replace('%',''), errors='coerce').fillna(0) / 100
    df['doc'] = 0 
    return df[['channel_sku', 'inventory', 'str', 'doc']]

def parse_blinkit(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['Item ID'].astype(str).str.strip()
    inv_df['inventory'] = pd.to_numeric(inv_df['Total sellable'], errors='coerce').fillna(0)
    
    if sales_df is not None:
        sales_df['Item Id'] = sales_df['Item Id'].astype(str).str.strip()
        sales_totals = sales_df.groupby('Item Id')['Quantity'].sum()
        inv_df = inv_df.merge(sales_totals, left_on='Item ID', right_index=True, how='left').fillna(0)
        inv_df['doc'] = inv_df['inventory'] / (inv_df['Quantity'] / 30).replace(0, 0.001)
        inv_df['str'] = inv_df.apply(lambda x: calculate_str(x['Quantity'], x['inventory']), axis=1)
    else:
        # Fallback to 30d column in inventory report
        sold_30 = pd.to_numeric(inv_df['Last 30 days'], errors='coerce').fillna(0)
        inv_df['doc'] = inv_df['inventory'] / (sold_30 / 30).replace(0, 0.001)
        inv_df['str'] = inv_df.apply(lambda x: calculate_str(sold_30, x['inventory']), axis=1)
    return inv_df[['channel_sku', 'inventory', 'doc', 'str']]

def parse_swiggy(inv_df, sales_df):
    sales_df['ITEM_CODE'] = sales_df['ITEM_CODE'].astype(str).str.strip()
    sales_totals = sales_df.groupby('ITEM_CODE')['UNITS_SOLD'].sum()
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SkuCode'].astype(str).str.strip()
    inv_df = inv_df.merge(sales_totals, left_on='SkuCode', right_index=True, how='left').fillna(0)
    inv_df['inventory'] = inv_df['WarehouseQtyAvailable']
    inv_df['doc'] = inv_df['inventory'] / (inv_df['UNITS_SOLD'] / 30).replace(0, 0.001)
    inv_df['str'] = inv_df.apply(lambda x: calculate_str(x['UNITS_SOLD'], x['inventory']), axis=1)
    return inv_df[['channel_sku', 'inventory', 'doc', 'str']]

def parse_bigbasket(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SKU_Id'].astype(str).str.strip()
    inv_df['inventory'] = pd.to_numeric(inv_df['Total SOH'], errors='coerce').fillna(0)
    
    if sales_df is not None:
        sales_df['source_sku_id'] = sales_df['source_sku_id'].astype(str).str.strip()
        sales_totals = sales_df.groupby('source_sku_id')['total_quantity'].sum()
        inv_df = inv_df.merge(sales_totals, left_on='SKU_Id', right_index=True, how='left').fillna(0)
        inv_df['doc'] = inv_df['inventory'] / (inv_df['total_quantity'] / 30).replace(0, 0.001)
        inv_df['str'] = inv_df.apply(lambda x: calculate_str(x['total_quantity'], x['inventory']), axis=1)
    else:
        inv_df['doc'] = pd.to_numeric(inv_df['SOH Day of Cover (HO)'], errors='coerce').fillna(0)
        inv_df['str'] = 0 # Cannot calculate STR without sales file for BB
    return inv_df[['channel_sku', 'inventory', 'doc', 'str']]

# --- MAIN APP ---

init_db()
master_list = pd.read_csv(COMMON_SKU_FILE)['name'].unique().tolist()
db_mappings = load_mapping_from_db()

st.title("🛡️ Mama Nourish | Cloud Inventory & STR Hub")

# 1. UPLOAD SECTION
st.subheader("📥 Step 1: Upload Reports")
c1, c2, c3, c4 = st.columns(4)
uploaded_data = []

with c1:
    st.info("**Amazon**")
    amz_i = st.file_uploader("Inventory (ASIN)", type="csv")
    if amz_i:
        uploaded_data.append((parse_amazon(pd.read_csv(amz_i, skiprows=1)), 'Amazon'))

with c2:
    st.info("**Blinkit**")
    blk_i = st.file_uploader("Blinkit Inventory", type="csv")
    blk_s = st.file_uploader("Blinkit Sales (MTD)", type="csv")
    if blk_i:
        s_df = pd.read_csv(blk_s) if blk_s else None
        uploaded_data.append((parse_blinkit(pd.read_csv(blk_i, skiprows=2), s_df), 'Blinkit'))

with c3:
    st.info("**Swiggy**")
    swg_i = st.file_uploader("Swiggy Inventory", type="csv")
    swg_s = st.file_uploader("Swiggy Sales", type="csv")
    if swg_i and swg_s:
        uploaded_data.append((parse_swiggy(pd.read_csv(swg_i), pd.read_csv(swg_s)), 'Swiggy'))

with c4:
    st.info("**Big Basket**")
    bb_i = st.file_uploader("BB Inventory", type="csv")
    bb_s = st.file_uploader("BB Sales (Alpha)", type="csv")
    if bb_i:
        s_df = pd.read_csv(bb_s) if bb_s else None
        uploaded_data.append((parse_bigbasket(pd.read_csv(bb_i), s_df), 'Big Basket'))

# 2. PROCESSING & CLOUD MAPPING
if uploaded_data:
    raw_dfs = [df.assign(channel=channel) for df, channel in uploaded_data]
    combined_raw = pd.concat(raw_dfs, ignore_index=True)
    merged = combined_raw.merge(db_mappings, on=['channel', 'channel_sku'], how='left')
    
    unmapped = merged[merged['master_sku'].isna()][['channel', 'channel_sku']].drop_duplicates()
    
    if not unmapped.empty:
        st.warning(f"🚨 {len(unmapped)} New SKUs found.")
        with st.form("db_mapping_form"):
            new_db_entries = []
            for _, row in unmapped.iterrows():
                chan, c_sku = row['channel'], row['channel_sku']
                choice = st.selectbox(f"Map {chan} ID: {c_sku}", ["Select..."] + master_list, key=f"{chan}_{c_sku}")
                if choice != "Select...":
                    new_db_entries.append({"channel": chan, "channel_sku": c_sku, "master_sku": choice})
            
            if st.form_submit_button("Save to Neon Cloud"):
                if new_db_entries:
                    save_mapping_to_db(new_db_entries)
                    st.rerun()
    else:
        # 3. FINAL DASHBOARD
        st.divider()
        st.subheader("📊 Global Health: Inventory, DoC & STR")
        
        # UI Formatting
        def style_metrics(df):
            return df.style.format({
                'str': '{:.2%}',
                'doc': '{:.1f}'
            }).background_gradient(subset=['doc'], cmap='RdYlGn'
            ).background_gradient(subset=['str'], cmap='YlGn')

        st.dataframe(style_metrics(merged[['master_sku', 'channel', 'inventory', 'doc', 'str']]), use_container_width=True)
else:
    st.info("Awaiting file uploads.")
