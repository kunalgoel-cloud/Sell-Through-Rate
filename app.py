import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io

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
    """Reads both CSV and Excel files."""
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

# --- CHANNEL PARSERS (CITY-LEVEL GRANULARITY) ---

def parse_amazon(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['ASIN'].astype(str).str.strip()
    inv_df['inventory'] = pd.to_numeric(inv_df['Sellable On Hand Units'], errors='coerce').fillna(0)
    inv_df['location'] = "National"
    
    # Try to get existing STR, else set to 0 to be calculated
    inv_df['str'] = 0.0
    if 'Sell-Through %' in inv_df.columns:
        inv_df['str'] = pd.to_numeric(inv_df['Sell-Through %'].astype(str).str.replace('%',''), errors='coerce').fillna(0) / 100
    
    if sales_df is not None:
        sales_df['ASIN'] = sales_df['ASIN'].astype(str).str.strip()
        sales_grp = sales_df.groupby('ASIN')['Ordered Units'].sum()
        inv_df = inv_df.merge(sales_grp, left_on='channel_sku', right_index=True, how='left').fillna(0)
        
        # Calculate STR if missing: Sales / (Sales + Inv)
        inv_df['str'] = inv_df.apply(lambda x: x['Ordered Units'] / (x['Ordered Units'] + x['inventory']) if (x['Ordered Units'] + x['inventory']) > 0 and x['str'] == 0 else x['str'], axis=1)
        inv_df['doc'] = inv_df['inventory'] / (inv_df['Ordered Units'] / 30).replace(0, 0.001)
    else:
        inv_df['doc'] = 0
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

def parse_blinkit(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['Item ID'].astype(str).str.strip()
    inv_df['fac_id'] = inv_df['Warehouse Facility Name'].astype(str).str.strip()
    inv_df['location'] = inv_df['fac_id']
    inv_df['inventory'] = pd.to_numeric(inv_df['Total sellable'], errors='coerce').fillna(0)
    
    if sales_df is not None:
        sales_df['Item Id'] = sales_df['Item Id'].astype(str).str.strip()
        sales_df['fac_id'] = sales_df['Facility Name'].astype(str).str.strip()
        sales_grp = sales_df.groupby(['Item Id', 'fac_id'])['Quantity'].sum().reset_index()
        inv_df = inv_df.merge(sales_grp, left_on=['channel_sku', 'fac_id'], right_on=['Item Id', 'fac_id'], how='left').fillna(0)
        sales_val = inv_df['Quantity']
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
    sales_df['ITEM_CODE'] = sales_df['ITEM_CODE'].astype(str).str.strip()
    sales_df['fac_id'] = sales_df['FACILITY_NAME'].astype(str).str.strip()
    sales_grp = sales_df.groupby(['ITEM_CODE', 'fac_id'])['UNITS_SOLD'].sum().reset_index()
    
    inv_df = inv_df.merge(sales_grp, left_on=['channel_sku', 'fac_id'], right_on=['ITEM_CODE', 'fac_id'], how='left').fillna(0)
    inv_df['inventory'] = pd.to_numeric(inv_df['WarehouseQtyAvailable'], errors='coerce').fillna(0)
    
    inv_df['str'] = inv_df['UNITS_SOLD'] / (inv_df['UNITS_SOLD'] + inv_df['inventory']).replace(0, 1)
    inv_df['doc'] = inv_df['inventory'] / (inv_df['UNITS_SOLD'] / 30).replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

def parse_bigbasket(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SKU_Id'].astype(str).str.strip()
    inv_df['fac_id'] = inv_df['DC'].astype(str).str.strip() if 'DC' in inv_df.columns else ""
    inv_df['location'] = inv_df['fac_id']
    inv_df['inventory'] = pd.to_numeric(inv_df['Total SOH'], errors='coerce').fillna(0)
    
    if sales_df is not None:
        sales_df['source_sku_id'] = sales_df['source_sku_id'].astype(str).str.strip()
        # Note: If BB sales has DC info, add fac_id to groupby here
        sales_grp = sales_df.groupby('source_sku_id')['total_quantity'].sum()
        inv_df = inv_df.merge(sales_grp, left_on='channel_sku', right_index=True, how='left').fillna(0)
        sales_val = inv_df['total_quantity']
    else:
        sales_val = 0
    
    inv_df['str'] = sales_val / (sales_val + inv_df['inventory']).replace(0, 1)
    inv_df['doc'] = inv_df['inventory'] / (sales_val / 30).replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

# --- MAIN APPLICATION ---
init_db()
master_list = pd.read_csv(COMMON_SKU_FILE)['name'].unique().tolist()
db_mappings = load_mapping_from_db()

st.title("🛡️ Mama Nourish | Global Inventory Hub")

# 1. UPLOAD SECTION
st.subheader("📥 Step 1: Upload Reports (CSV/Excel)")
c1, c2, c3, c4 = st.columns(4)
uploaded_data = []
f_types = ["csv", "xlsx", "xls"]

with c1:
    st.info("**Amazon**")
    amz_i = st.file_uploader("Amazon Inventory", type=f_types)
    amz_s = st.file_uploader("Amazon Sales", type=f_types)
    if amz_i:
        s = load_data(amz_s, skiprows=1) if amz_s else None
        uploaded_data.append((parse_amazon(load_data(amz_i, skiprows=1), s), 'Amazon'))

with c2:
    st.info("**Blinkit**")
    blk_i = st.file_uploader("Blinkit Inventory", type=f_types)
    blk_s = st.file_uploader("Blinkit Sales", type=f_types)
    if blk_i:
        s = load_data(blk_s) if blk_s else None
        uploaded_data.append((parse_blinkit(load_data(blk_i, skiprows=2), s), 'Blinkit'))

with c3:
    st.info("**Swiggy**")
    swg_i = st.file_uploader("Swiggy Inventory", type=f_types)
    swg_s = st.file_uploader("Swiggy Sales", type=f_types)
    if swg_i and swg_s:
        uploaded_data.append((parse_swiggy(load_data(swg_i), load_data(swg_s)), 'Swiggy'))

with c4:
    st.info("**Big Basket**")
    bb_i = st.file_uploader("BB Inventory", type=f_types)
    bb_s = st.file_uploader("BB Sales", type=f_types)
    if bb_i:
        s = load_data(bb_s) if bb_s else None
        uploaded_data.append((parse_bigbasket(load_data(bb_i), s), 'Big Basket'))

# 2. DATA PROCESSING & MAPPING
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
        st.warning(f"🚨 {len(unmapped)} New SKUs found. Map them to continue.")
        with st.form("map_form"):
            new_entries = []
            for _, row in unmapped.iterrows():
                choice = st.selectbox(f"{row['channel']}: {row['channel_sku']}", ["Select..."]+master_list)
                if choice != "Select...":
                    new_entries.append({"channel": row['channel'], "channel_sku": row['channel_sku'], "master_sku": choice})
            if st.form_submit_button("Save Mappings"):
                save_mapping_to_db(new_entries)
                st.rerun()
    else:
        # 3. FILTERS & METRICS
        st.sidebar.header("🔍 Filters")
        sel_chan = st.sidebar.multiselect("Channels", sorted(merged['channel'].unique()), default=sorted(merged['channel'].unique()))
        sel_prod = st.sidebar.multiselect("Products", sorted(merged['master_sku'].unique()), default=sorted(merged['master_sku'].unique()))
        
        filtered = merged[(merged['channel'].isin(sel_chan)) & (merged['master_sku'].isin(sel_prod))]

        st.divider()
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Inventory", f"{filtered['inventory'].sum():,.0f}")
        m2.metric("Avg Days of Cover", f"{filtered[filtered['doc']>0]['doc'].mean():.1f} days")
        m3.metric("Avg Sell-Through", f"{filtered[filtered['str']>0]['str'].mean():.2%}")

        # 4. FINAL DASHBOARD
        def color_doc(val):
            color = 'red' if val < 7 else 'orange' if val < 15 else 'green'
            return f'color: {color}'

        st.subheader("📊 Inventory Performance by Location")
        display_df = filtered[['master_sku', 'channel', 'location', 'inventory', 'doc', 'str']].sort_values('doc')
        
        st.dataframe(
            display_df.style.format({'str': '{:.2%}', 'doc': '{:.1f}', 'inventory': '{:,.0f}'})
            .applymap(color_doc, subset=['doc']),
            use_container_width=True
        )
else:
    st.info("Please upload your inventory and sales reports to generate the dashboard.")
