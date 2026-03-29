import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURATION & DATABASE ---
st.set_page_config(page_title="Mama Nourish | Inventory Control", layout="wide")

try:
    conn_url = st.secrets["connections"]["postgresql"]["url"]
    engine = create_engine(conn_url)
except Exception:
    st.error("Database connection string not found. Check your Streamlit Secrets.")
    st.stop()

COMMON_SKU_FILE = "2026-03-28T16-00_export.csv"

# --- HELPER FUNCTIONS ---
def load_data(uploaded_file, skiprows=0):
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file, skiprows=skiprows)
    else:
        return pd.read_excel(uploaded_file, skiprows=skiprows)

def find_col(df, options):
    for opt in options:
        if opt in df.columns:
            return opt
    return None

def init_db():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sku_mappings (
                id SERIAL PRIMARY KEY, channel TEXT NOT NULL, 
                channel_sku TEXT NOT NULL, master_sku TEXT NOT NULL,
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

# --- CHANNEL PARSERS (FIXED ATTRIBUTE ERRORS) ---

def parse_amazon(inv_df, sales_df=None, sales_filename=None):
    import re
    inv_df = inv_df.copy()
    sku_c = find_col(inv_df, ['ASIN', 'asin', 'sku'])
    inv_df['channel_sku'] = inv_df[sku_c].astype(str).str.strip() if sku_c else ""
    inv_df['inventory'] = pd.to_numeric(inv_df['Sellable On Hand Units'], errors='coerce').fillna(0)
    inv_df['location'] = "National"

    # Amazon provides Sell-Through % in the inventory report — use it directly
    inv_df['str'] = 0.0
    if 'Sell-Through %' in inv_df.columns:
        inv_df['str'] = pd.to_numeric(
            inv_df['Sell-Through %'].astype(str).str.replace('%', ''), errors='coerce'
        ).fillna(0) / 100

    # Detect date span from sales filename (pattern: D-M-YYYY_D-M-YYYY) for accurate DOC
    n_days = 30  # safe default
    if sales_filename:
        dates = re.findall(r'(\d{1,2}-\d{1,2}-\d{4})', sales_filename)
        if len(dates) == 2:
            try:
                d1 = pd.to_datetime(dates[0], dayfirst=True)
                d2 = pd.to_datetime(dates[1], dayfirst=True)
                n_days = max((d2 - d1).days + 1, 1)
            except Exception:
                pass

    sales_val = pd.Series(0.0, index=inv_df.index)
    if sales_df is not None:
        s_sku = find_col(sales_df, ['ASIN', 'asin'])
        if s_sku:
            sales_df = sales_df.copy()
            sales_df[s_sku] = sales_df[s_sku].astype(str).str.strip()
            sales_grp = sales_df.groupby(s_sku)['Ordered Units'].sum()
            inv_df = inv_df.merge(sales_grp, left_on='channel_sku', right_index=True, how='left').fillna(0)
            sales_val = pd.to_numeric(inv_df['Ordered Units'], errors='coerce').fillna(0)

    # DOC uses actual day span; STR stays as Amazon-reported value (more accurate than recomputing)
    inv_df['doc'] = inv_df['inventory'] / (sales_val / n_days).replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

def parse_blinkit(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['Item ID'].astype(str).str.strip()
    f_col = find_col(inv_df, ['Warehouse Facility Name', 'Facility Name', 'Store'])
    inv_df['fac_id'] = inv_df[f_col].astype(str).str.strip() if f_col else "Unknown"
    inv_df['location'] = inv_df['fac_id']
    inv_df['inventory'] = pd.to_numeric(inv_df['Total sellable'], errors='coerce').fillna(0)
    
    sales_val = pd.Series(0, index=inv_df.index)
    if sales_df is not None:
        s_sku = find_col(sales_df, ['Item Id', 'item_id'])
        s_fac = find_col(sales_df, ['Facility Name', 'Store', 'Warehouse'])
        if s_sku and s_fac:
            sales_df[s_sku] = sales_df[s_sku].astype(str).str.strip()
            sales_df['fac_id'] = sales_df[s_fac].astype(str).str.strip()
            sales_grp = sales_df.groupby([s_sku, 'fac_id'])['Quantity'].sum().reset_index()
            inv_df = inv_df.merge(sales_grp, left_on=['channel_sku', 'fac_id'], right_on=[s_sku, 'fac_id'], how='left').fillna(0)
            sales_val = inv_df['Quantity']
    else:
        sales_val = pd.to_numeric(inv_df['Last 30 days'], errors='coerce').fillna(0)
    
    inv_df['str'] = sales_val / (sales_val + inv_df['inventory']).replace(0, 1)
    inv_df['doc'] = inv_df['inventory'] / (sales_val / 30).replace(0, 0.001)
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

def parse_swiggy(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SkuCode'].astype(str).str.strip()
    inv_df['fac_id'] = inv_df['FacilityName'].astype(str).str.strip()
    inv_df['location'] = inv_df['City'] + " (" + inv_df['FacilityName'] + ")"
    inv_df['inventory'] = pd.to_numeric(inv_df['WarehouseQtyAvailable'], errors='coerce').fillna(0)
    # Normalise city for joining (sales file has CITY, not FacilityName)
    inv_df['_city_key'] = inv_df['City'].astype(str).str.strip().str.upper()

    # DaysOnHand from inventory file = Swiggy's own pre-computed DOC; cap at 365 to remove outliers
    doh_fallback = pd.to_numeric(inv_df['DaysOnHand'], errors='coerce').fillna(0).clip(upper=365) \
        if 'DaysOnHand' in inv_df.columns else pd.Series(0.0, index=inv_df.index)

    sales_val = pd.Series(0.0, index=inv_df.index)
    n_days = 30  # default; overridden below when a date column is present
    has_sales = False

    if sales_df is not None:
        sales_df = sales_df.copy()
        s_sku = find_col(sales_df, ['ITEM_CODE', 'ItemCode', 'SKU'])
        # Sales report uses CITY, not FacilityName — join on SKU + City
        s_city = find_col(sales_df, ['CITY', 'City', 'city'])
        # Detect actual date span so DOC & STR are correctly normalised to 30 days
        date_col = find_col(sales_df, ['ORDERED_DATE', 'OrderedDate', 'Date', 'date'])
        if date_col:
            dates = pd.to_datetime(sales_df[date_col], errors='coerce').dropna()
            if not dates.empty:
                n_days = max((dates.max() - dates.min()).days + 1, 1)
        if s_sku and s_city:
            sales_df['c_sku'] = sales_df[s_sku].astype(str).str.strip()
            sales_df['c_city'] = sales_df[s_city].astype(str).str.strip().str.upper()
            sales_grp = sales_df.groupby(['c_sku', 'c_city'])['UNITS_SOLD'].sum().reset_index()
            inv_df = inv_df.merge(sales_grp, left_on=['channel_sku', '_city_key'],
                                  right_on=['c_sku', 'c_city'], how='left').fillna(0)
            sales_val = inv_df['UNITS_SOLD']
            has_sales = True

    if has_sales:
        # Sales file uploaded: compute DOC & STR from actual sales, normalised to day span
        daily_rate = (sales_val / n_days).replace(0, 0.001)
        sales_30d = sales_val * (30 / n_days)
        inv_df['str'] = sales_30d / (sales_30d + inv_df['inventory']).replace(0, 1)
        # For locations with zero sales in the window, fall back to DaysOnHand
        computed_doc = inv_df['inventory'] / daily_rate
        inv_df['doc'] = computed_doc.where(sales_val > 0, doh_fallback.values)
    else:
        # No sales file: use Swiggy's own DaysOnHand as DOC, STR unavailable
        inv_df['doc'] = doh_fallback.values
        inv_df['str'] = 0.0

    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'location']]

def parse_bigbasket(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SKU_Id'].astype(str).str.strip()
    inv_df['location'] = inv_df['DC'].astype(str).str.strip() if 'DC' in inv_df.columns else "Unknown"
    inv_df['inventory'] = pd.to_numeric(inv_df['Total SOH'], errors='coerce').fillna(0)
    
    sales_val = pd.Series(0, index=inv_df.index)
    if sales_df is not None:
        s_sku = find_col(sales_df, ['source_sku_id', 'SKU_Id', 'SKU ID'])
        if s_sku:
            sales_df[s_sku] = sales_df[s_sku].astype(str).str.strip()
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

st.title("🛡️ Mama Nourish | Inventory Control Hub")

# --- 1. UPLOAD (CLEAR SEPARATION) ---
st.subheader("📥 Upload Reports")
c1, c2, c3, c4 = st.columns(4)
uploaded_data = []
f_types = ["csv", "xlsx", "xls"]

with c1:
    st.info("**Amazon**")
    ai = st.file_uploader("Amazon Inventory", type=f_types, key="amz_i")
    as_ = st.file_uploader("Amazon Sales", type=f_types, key="amz_s")
    if ai:
        uploaded_data.append((
            parse_amazon(load_data(ai, 1), load_data(as_, 1) if as_ else None,
                         sales_filename=as_.name if as_ else None),
            'Amazon'
        ))

with c2:
    st.info("**Blinkit**")
    bi = st.file_uploader("Blinkit Inventory", type=f_types, key="blk_i")
    bs = st.file_uploader("Blinkit Sales", type=f_types, key="blk_s")
    if bi: uploaded_data.append((parse_blinkit(load_data(bi, 2), load_data(bs) if bs else None), 'Blinkit'))

with c3:
    st.info("**Swiggy**")
    si = st.file_uploader("Swiggy Inventory", type=f_types, key="swg_i")
    ss = st.file_uploader("Swiggy Sales", type=f_types, key="swg_s")
    if si: uploaded_data.append((parse_swiggy(load_data(si), load_data(ss) if ss else None), 'Swiggy'))

with c4:
    st.info("**Big Basket**")
    bbi = st.file_uploader("BB Inventory", type=f_types, key="bb_i")
    bbs = st.file_uploader("BB Sales", type=f_types, key="bb_s")
    if bbi: uploaded_data.append((parse_bigbasket(load_data(bbi), load_data(bbs) if bbs else None), 'Big Basket'))

# --- 2. DATA MERGING & MAPPING ---
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
                choice = st.selectbox(f"{row['channel']}: {row['channel_sku']}", ["Select..."] + master_list)
                if choice != "Select...": 
                    new_entries.append({"channel": row['channel'], "channel_sku": row['channel_sku'], "master_sku": choice})
            if st.form_submit_button("Save & Sync"):
                save_mapping_to_db(new_entries)
                st.rerun()
    else:
        # --- 3. SIDEBAR FILTERS ---
        st.sidebar.header("🔍 Global Filters")
        u_channels = sorted(merged['channel'].unique().tolist())
        sel_channels = st.sidebar.multiselect("Filter by Channel", u_channels, default=u_channels)
        
        u_products = sorted(merged['master_sku'].dropna().unique().tolist())
        sel_products = st.sidebar.multiselect("Filter by Product", u_products, default=u_products)

        filtered_df = merged[(merged['channel'].isin(sel_channels)) & (merged['master_sku'].isin(sel_products))]

        # --- 4. TOP LINE METRICS (WEIGHTED AVERAGES) ---
        st.divider()
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Inventory", f"{filtered_df['inventory'].sum():,.0f} units")

        # Weighted avg DOC: exclude rows where doc is sentinel-inflated (zero sales)
        valid_doc_data = filtered_df[(filtered_df['doc'] > 0) & (filtered_df['doc'] < 9999)]
        if not valid_doc_data.empty and valid_doc_data['inventory'].sum() > 0:
            weighted_doc = (valid_doc_data['doc'] * valid_doc_data['inventory']).sum() / valid_doc_data['inventory'].sum()
            m2.metric("Avg Days of Cover", f"{weighted_doc:.1f} days")
        else:
            m2.metric("Avg Days of Cover", "N/A")

        # Weighted avg STR: use all rows with inventory (STR=0 is valid for unsold SKUs)
        str_data = filtered_df[filtered_df['inventory'] > 0]
        if not str_data.empty:
            weighted_str = (str_data['str'] * str_data['inventory']).sum() / str_data['inventory'].sum()
            m3.metric("Avg Sell-Through %", f"{weighted_str:.2%}")
        else:
            m3.metric("Avg Sell-Through %", "0.00%")

        # --- 5. DASHBOARD TABLE ---
        st.subheader("📊 Inventory Performance by Location")
        def color_doc(val):
            if val < 7:
                return 'color: red; font-weight: bold'
            elif val < 15:
                return 'color: orange; font-weight: bold'
            else:
                return ''  # inherit default — 'white' was invisible on white background

        display_cols = ['master_sku', 'channel', 'location', 'inventory', 'doc', 'str']
        st.dataframe(
            filtered_df[display_cols].sort_values('doc').style.format({
                'str': '{:.2%}', 'doc': '{:.1f}', 'inventory': '{:,.0f}'
            }).applymap(color_doc, subset=['doc']),
            use_container_width=True
        )
else:
    st.info("Upload channel files to generate the dashboard.")
