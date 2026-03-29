import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURATION & DATABASE ---
st.set_page_config(page_title="Mama Nourish | Inventory Control Center", layout="wide")

try:
    conn_url = st.secrets["connections"]["postgresql"]["url"]
    engine = create_engine(conn_url)
except Exception:
    st.error("Database connection string not found in Streamlit Secrets.")
    st.stop()

COMMON_SKU_FILE = "2026-03-28T16-00_export.csv"

# --- HELPER FUNCTIONS ---
def load_data(uploaded_file, skiprows=0):
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file, skiprows=skiprows)
    else:
        return pd.read_excel(uploaded_file, skiprows=skiprows)

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

# --- ROBUST COLUMN FINDER ---
def find_col(df, options):
    """Finds the first matching column name from a list of options."""
    for opt in options:
        if opt in df.columns:
            return opt
    return None

# --- CHANNEL PARSERS ---

def parse_amazon(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    sku_col = find_col(inv_df, ['ASIN', 'asin', 'sku'])
    inv_df['channel_sku'] = inv_df[sku_col].astype(str).str.strip() if sku_col else ""
    inv_df['inventory'] = pd.to_numeric(inv_df['Sellable On Hand Units'], errors='coerce').fillna(0)
    inv_df['location'] = "National"
    inv_df['str'] = 0.0
    if 'Sell-Through %' in inv_df.columns:
        inv_df['str'] = pd.to_numeric(inv_df['Sell-Through %'].astype(str).str.replace('%',''), errors='coerce').fillna(0) / 100
    
    if sales_df is not None:
        s_sku = find_col(sales_df, ['ASIN', 'asin'])
        if s_sku:
            sales_df[s_sku] = sales_df[s_sku].astype(str).str.strip()
            sales_grp = sales_df.groupby(s_sku)['Ordered Units'].sum()
            inv_df = inv_df.merge(sales_grp, left_on='channel_sku', right_index=True, how='left').fillna(0)
            inv_df['str'] = inv_df.apply(lambda x: x['Ordered Units'] / (x['Ordered Units'] + x['inventory']) if (x['Ordered Units'] + x['inventory']) > 0 and x['str'] == 0 else x['str'], axis=1)
            inv_df['doc'] = inv_df['inventory'] / (inv_df['Ordered Units'] / 30).replace(0, 0.001)
    else:
        inv_df['doc'] = 0
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

def parse_blinkit(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['Item ID'].astype(str).str.strip()
    fac_col = find_col(inv_df, ['Warehouse Facility Name', 'Facility Name', 'Store'])
    inv_df['fac_id'] = inv_df[fac_col].astype(str).str.strip() if fac_col else "Unknown"
    inv_df['location'] = inv_df['fac_id']
    inv_df['inventory'] = pd.to_numeric(inv_df['Total sellable'], errors='coerce').fillna(0)
    
    if sales_df is not None:
        s_sku = find_col(sales_df, ['Item Id', 'item_id'])
        s_fac = find_col(sales_df, ['Facility Name', 'store_name', 'Warehouse'])
        if s_sku and s_fac:
            sales_df[s_sku] = sales_df[s_sku].astype(str).str.strip()
            sales_df['fac_id'] = sales_df[s_fac].astype(str).str.strip()
            sales_grp = sales_df.groupby([s_sku, 'fac_id'])['Quantity'].sum().reset_index()
            inv_df = inv_df.merge(sales_grp, left_on=['channel_sku', 'fac_id'], right_on=[s_sku, 'fac_id'], how='left').fillna(0)
            sales_val = inv_df['Quantity']
        else: sales_val = 0
    else:
        sales_val = pd.to_numeric(inv_df['Last 30 days'], errors='coerce').fillna(0)
    
    inv_df['str'] = sales_val / (sales_val + inv_df['inventory']).replace(0, 1)
    inv_df['doc'] = inv_df['inventory'] / (sales_val / 30).replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

def parse_swiggy(inv_df, sales_df):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SkuCode'].astype(str).str.strip()
    inv_df['fac_id'] = inv_df['FacilityName'].astype(str).str.strip()
    inv_df['location'] = inv_df['City'] + " (" + inv_df['FacilityName'] + ")"
    
    sales_df = sales_df.copy()
    # Handle the KeyError by looking for possible column name variations
    s_sku = find_col(sales_df, ['ITEM_CODE', 'ItemCode', 'SKU'])
    s_fac = find_col(sales_df, ['FACILITY_NAME', 'FacilityName', 'Warehouse', 'Store'])
    
    if s_sku and s_fac:
        sales_df['clean_sku'] = sales_df[s_sku].astype(str).str.strip()
        sales_df['clean_fac'] = sales_df[s_fac].astype(str).str.strip()
        sales_grp = sales_df.groupby(['clean_sku', 'clean_fac'])['UNITS_SOLD'].sum().reset_index()
        
        inv_df = inv_df.merge(sales_grp, left_on=['channel_sku', 'fac_id'], right_on=['clean_sku', 'clean_fac'], how='left').fillna(0)
        sales_val = inv_df['UNITS_SOLD']
    else:
        st.error(f"Swiggy Sales columns not found. Expected {s_sku} and {s_fac}")
        sales_val = 0

    inv_df['inventory'] = pd.to_numeric(inv_df['WarehouseQtyAvailable'], errors='coerce').fillna(0)
    inv_df['str'] = sales_val / (sales_val + inv_df['inventory']).replace(0, 1)
    inv_df['doc'] = inv_df['inventory'] / (sales_val / 30).replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

def parse_bigbasket(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SKU_Id'].astype(str).str.strip()
    inv_df['location'] = inv_df['DC'].astype(str).str.strip() if 'DC' in inv_df.columns else "Unknown"
    inv_df['inventory'] = pd.to_numeric(inv_df['Total SOH'], errors='coerce').fillna(0)
    sales_val = 0
    if sales_df is not None:
        s_sku = find_col(sales_df, ['source_sku_id', 'SKU_Id'])
        if s_sku:
            sales_grp = sales_df.groupby(s_sku)['total_quantity'].sum()
            inv_df = inv_df.merge(sales_grp, left_on='channel_sku', right_index=True, how='left').fillna(0)
            sales_val = inv_df['total_quantity']
    
    inv_df['str'] = sales_val / (sales_val + inv_df['inventory']).replace(0, 1)
    inv_df['doc'] = inv_df['inventory'] / (sales_val / 30).replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

# --- MAIN APP ---
init_db()
master_list = pd.read_csv(COMMON_SKU_FILE)['name'].unique().tolist()
db_mappings = load_mapping_from_db()

st.title("🛡️ Mama Nourish | Global Inventory Hub")

# 1. UPLOAD
c1, c2, c3, c4 = st.columns(4)
uploaded_data = []
f_types = ["csv", "xlsx", "xls"]

with c1:
    st.info("**Amazon**")
    amz_i = st.file_uploader("Amazon Inv", type=f_types)
    amz_s = st.file_uploader("Amazon Sales", type=f_types)
    if amz_i:
        uploaded_data.append((parse_amazon(load_data(amz_i, 1), load_data(amz_s, 1) if amz_s else None), 'Amazon'))

with c2:
    st.info("**Blinkit**")
    blk_i = st.file_uploader("Blinkit Inv", type=f_types)
    blk_s = st.file_uploader("Blinkit Sales", type=f_types)
    if blk_i:
        uploaded_data.append((parse_blinkit(load_data(blk_i, 2), load_data(blk_s) if blk_s else None), 'Blinkit'))

with c3:
    st.info("**Swiggy**")
    swg_i = st.file_uploader("Swiggy Inv", type=f_types)
    swg_s = st.file_uploader("Swiggy Sales", type=f_types)
    if swg_i and swg_s:
        uploaded_data.append((parse_swiggy(load_data(swg_i), load_data(swg_s)), 'Swiggy'))

with c4:
    st.info("**Big Basket**")
    bb_i = st.file_uploader("BB Inv", type=f_types)
    bb_s = st.file_uploader("BB Sales", type=f_types)
    if bb_i:
        uploaded_data.append((parse_bigbasket(load_data(bb_i), load_data(bb_s) if bb_s else None), 'Big Basket'))

# 2. PROCESS
if uploaded_data:
    all_dfs = []
    for df, channel in uploaded_data:
        df['channel'] = channel
        all_dfs.append(df)
    combined = pd.concat(all_dfs, ignore_index=True)
    combined['channel_sku'] = combined['channel_sku'].astype(str)
    db_mappings['channel_sku'] = db_mappings['channel_sku'].astype(str)
    
    merged = combined.merge(db_mappings, on=['channel', 'channel_sku'], how='left')
    unmapped = merged[merged['master_sku'].isna()][['channel', 'channel_sku']].drop_duplicates()
    
    if not unmapped.empty:
        with st.form("map_form"):
            new_entries = []
            for _, row in unmapped.iterrows():
                choice = st.selectbox(f"{row['channel']}: {row['channel_sku']}", ["Select..."]+master_list)
                if choice != "Select...": new_entries.append({"channel": row['channel'], "channel_sku": row['channel_sku'], "master_sku": choice})
            if st.form_submit_button("Save"):
                save_mapping_to_db(new_entries)
                st.rerun()
    else:
        # 3. DISPLAY
        st.sidebar.header("🔍 Filters")
        sel_chan = st.sidebar.multiselect("Channels", sorted(merged['channel'].unique()), default=sorted(merged['channel'].unique()))
        sel_prod = st.sidebar.multiselect("Products", sorted(merged['master_sku'].unique()), default=sorted(merged['master_sku'].unique()))
        
        filtered = merged[(merged['channel'].isin(sel_chan)) & (merged['master_sku'].isin(sel_prod))]
        
        st.divider()
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Inventory", f"{filtered['inventory'].sum():,.0f}")
        m2.metric("Avg Days of Cover", f"{filtered[filtered['doc']>0]['doc'].mean():.1f}")
        m3.metric("Avg STR%", f"{filtered[filtered['str']>0]['str'].mean():.2%}")

        st.dataframe(filtered[['master_sku', 'channel', 'location', 'inventory', 'doc', 'str']].sort_values('doc').style.format({'str': '{:.2%}', 'doc': '{:.1f}'}), use_container_width=True)
else:
    st.info("Upload files to start.")
