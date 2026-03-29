import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- DATABASE & CONFIG ---
st.set_page_config(page_title="Mama Nourish | Inventory Control", layout="wide")

try:
    conn_url = st.secrets["connections"]["postgresql"]["url"]
    engine = create_engine(conn_url)
except Exception:
    st.error("Database connection string not found. Please add it to Streamlit Secrets.")
    st.stop()

COMMON_SKU_FILE = "2026-03-28T16-00_export.csv"

# --- HELPER: FILE LOADER ---
def load_data(uploaded_file, skiprows=0):
    """Helper to read both CSV and Excel files"""
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file, skiprows=skiprows)
    else:
        # For Excel, skiprows works the same way
        return pd.read_excel(uploaded_file, skiprows=skiprows)

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
            conn.execute(text("""
                INSERT INTO sku_mappings (channel, channel_sku, master_sku)
                VALUES (:channel, :channel_sku, :master_sku)
                ON CONFLICT (channel, channel_sku) DO UPDATE SET master_sku = EXCLUDED.master_sku
            """), entry)
        conn.commit()

# --- CHANNEL PARSERS ---
def parse_amazon(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['ASIN'].astype(str).str.strip()
    inv_df['inventory'] = pd.to_numeric(inv_df['Sellable On Hand Units'], errors='coerce').fillna(0)
    inv_df['str'] = pd.to_numeric(inv_df['Sell-Through %'].astype(str).str.replace('%',''), errors='coerce').fillna(0) / 100
    
    if sales_df is not None:
        sales_df['ASIN'] = sales_df['ASIN'].astype(str).str.strip()
        # Amazon Sales DRR based on report period (standardized to 30 days)
        drr = sales_df.groupby('ASIN')['Ordered Units'].sum() / 30
        inv_df = inv_df.merge(drr, left_on='ASIN', right_index=True, how='left')
        inv_df['doc'] = inv_df['inventory'] / inv_df['Ordered Units'].replace(0, 0.001)
    else:
        inv_df['doc'] = 0
    return inv_df[['channel_sku', 'inventory', 'str', 'doc']]

def parse_blinkit(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['Item ID'].astype(str).str.strip()
    inv_df['inventory'] = pd.to_numeric(inv_df['Total sellable'], errors='coerce').fillna(0)
    if sales_df is not None:
        sales_df['Item Id'] = sales_df['Item Id'].astype(str).str.strip()
        drr = sales_df.groupby('Item Id')['Quantity'].sum() / 30
        inv_df = inv_df.merge(drr, left_on='Item ID', right_index=True, how='left')
        inv_df['doc'] = inv_df['inventory'] / inv_df['Quantity'].replace(0, 0.001)
    else:
        drr_col = pd.to_numeric(inv_df['Last 30 days'], errors='coerce').fillna(0) / 30
        inv_df['doc'] = inv_df['inventory'] / drr_col.replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'doc']]

def parse_swiggy(inv_df, sales_df):
    sales_df['ITEM_CODE'] = sales_df['ITEM_CODE'].astype(str).str.strip()
    drr = sales_df.groupby('ITEM_CODE')['UNITS_SOLD'].sum() / 30
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SkuCode'].astype(str).str.strip()
    inv_df = inv_df.merge(drr, left_on='SkuCode', right_index=True, how='left').fillna(0)
    inv_df['inventory'] = inv_df['WarehouseQtyAvailable']
    inv_df['doc'] = inv_df['inventory'] / inv_df['UNITS_SOLD'].replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'doc']]

def parse_bigbasket(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SKU_Id'].astype(str).str.strip()
    inv_df['inventory'] = pd.to_numeric(inv_df['Total SOH'], errors='coerce').fillna(0)
    if sales_df is not None:
        sales_df['source_sku_id'] = sales_df['source_sku_id'].astype(str).str.strip()
        drr = sales_df.groupby('source_sku_id')['total_quantity'].sum() / 30
        inv_df = inv_df.merge(drr, left_on='SKU_Id', right_index=True, how='left')
        inv_df['doc'] = inv_df['inventory'] / inv_df['total_quantity'].replace(0, 0.001)
    else:
        inv_df['doc'] = pd.to_numeric(inv_df['SOH Day of Cover (HO)'], errors='coerce').fillna(0)
    return inv_df[['channel_sku', 'inventory', 'doc']]

# --- MAIN APP ---
init_db()
master_list = pd.read_csv(COMMON_SKU_FILE)['name'].unique().tolist()
db_mappings = load_mapping_from_db()

st.title("🛡️ Mama Nourish | Control Center")

# --- STEP 1: UPLOAD (CSV & EXCEL) ---
st.subheader("📥 Upload Reports")
c1, c2, c3, c4 = st.columns(4)
uploaded_data = []
file_types = ["csv", "xlsx", "xls"]

with c1:
    st.info("**Amazon**")
    amz_i = st.file_uploader("Amazon Inventory", type=file_types)
    amz_s = st.file_uploader("Amazon Sales", type=file_types)
    if amz_i:
        s_df = load_data(amz_s, skiprows=1) if amz_s else None
        uploaded_data.append((parse_amazon(load_data(amz_i, skiprows=1), s_df), 'Amazon'))

with c2:
    st.info("**Blinkit**")
    blk_i = st.file_uploader("Blinkit Inventory", type=file_types)
    blk_s = st.file_uploader("Blinkit Sales", type=file_types)
    if blk_i:
        s_df = load_data(blk_s) if blk_s else None
        uploaded_data.append((parse_blinkit(load_data(blk_i, skiprows=2), s_df), 'Blinkit'))

with c3:
    st.info("**Swiggy**")
    swg_i = st.file_uploader("Swiggy Inventory", type=file_types)
    swg_s = st.file_uploader("Swiggy Sales", type=file_types)
    if swg_i and swg_s:
        uploaded_data.append((parse_swiggy(load_data(swg_i), load_data(swg_s)), 'Swiggy'))

with c4:
    st.info("**Big Basket**")
    bb_i = st.file_uploader("BB Inventory", type=file_types)
    bb_s = st.file_uploader("BB Sales", type=file_types)
    if bb_i:
        s_df = load_data(bb_s) if bb_s else None
        uploaded_data.append((parse_bigbasket(load_data(bb_i), s_df), 'Big Basket'))

# --- STEP 2: PROCESSING & FILTERS ---
if uploaded_data:
    all_dfs = []
    for df, channel in uploaded_data:
        df['channel'] = channel
        all_dfs.append(df)
    
    combined = pd.concat(all_dfs, ignore_index=True)
    merged = combined.merge(db_mappings, on=['channel', 'channel_sku'], how='left')
    
    unmapped = merged[merged['master_sku'].isna()][['channel', 'channel_sku']].drop_duplicates()
    
    if not unmapped.empty:
        st.warning(f"🚨 {len(unmapped)} New SKUs detected.")
        with st.form("mapping_form"):
            new_db_entries = []
            for _, row in unmapped.iterrows():
                chan, c_sku = row['channel'], row['channel_sku']
                choice = st.selectbox(f"Map {chan}: {c_sku}", ["Select..."] + master_list, key=f"{chan}_{c_sku}")
                if choice != "Select...":
                    new_db_entries.append({"channel": chan, "channel_sku": c_sku, "master_sku": choice})
            if st.form_submit_button("Sync to Neon Cloud"):
                if new_db_entries:
                    save_mapping_to_db(new_db_entries)
                    st.success("Database Updated!")
                    st.rerun()
    else:
        # --- FILTERS ---
        st.sidebar.header("🔍 Filter View")
        channels = ["All"] + sorted(merged['channel'].unique().tolist())
        sel_channel = st.sidebar.selectbox("Channel", channels)
        
        products = ["All"] + sorted(merged['master_sku'].unique().tolist())
        sel_product = st.sidebar.selectbox("Product", products)

        filtered_df = merged.copy()
        if sel_channel != "All":
            filtered_df = filtered_df[filtered_df['channel'] == sel_channel]
        if sel_product != "All":
            filtered_df = filtered_df[filtered_df['master_sku'] == sel_product]

        # --- DASHBOARD ---
        st.divider()
        st.subheader("📊 Global Inventory Health")
        
        for col in ['doc', 'str']:
            if col not in filtered_df.columns: filtered_df[col] = 0

        def style_metrics(df):
            return df.style.format({
                'str': '{:.2%}',
                'inventory': '{:.0f}',
                'doc': '{:.1f} days'
            }).background_gradient(subset=['doc'], cmap='RdYlGn_r')

        display_cols = ['master_sku', 'channel', 'inventory', 'doc', 'str']
        st.dataframe(style_metrics(filtered_df[display_cols].sort_values('doc')), use_container_width=True)
else:
    st.info("Upload CSV or Excel exports to begin.")
