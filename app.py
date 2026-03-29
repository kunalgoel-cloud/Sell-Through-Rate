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
    inv_df['drr'] = (sales_val / n_days).round(2)
    inv_df['doc'] = inv_df['inventory'] / inv_df['drr'].replace(0, 0.001)
    inv_df['units_sold'] = sales_val
    inv_df['n_days'] = n_days
    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'drr', 'units_sold', 'n_days', 'location']]

def parse_blinkit(inv_df, sales_df=None):
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['Item ID'].astype(str).str.strip()
    f_col = find_col(inv_df, ['Warehouse Facility Name', 'Facility Name', 'Store'])
    inv_df['fac_id'] = inv_df[f_col].astype(str).str.strip() if f_col else "Unknown"
    inv_df['location'] = inv_df['fac_id']
    inv_df['inventory'] = pd.to_numeric(inv_df['Total sellable'], errors='coerce').fillna(0)

    # Extract city from facility name (e.g. 'Pune P2 - Feeder' -> 'Pune')
    # and map NCR sub-regions to their Blinkit Supply City codes
    NCR_CITY_MAP = {
        'Farukhnagar': 'HR-NCR', 'Kundli': 'HR-NCR', 'Faridabad': 'HR-NCR',
        'Gurgaon': 'HR-NCR', 'Gurugram': 'HR-NCR',
        'Noida': 'UP-NCR', 'Ghaziabad': 'UP-NCR', 'Gr.Noida': 'UP-NCR',
    }
    def extract_city(facility_name):
        first_word = str(facility_name).split()[0]
        return NCR_CITY_MAP.get(first_word, first_word)

    inv_df['_city_key'] = inv_df['fac_id'].apply(extract_city)

    n_days = 30  # default; overridden from date column when sales present
    has_sales = False
    sales_val = pd.Series(0.0, index=inv_df.index)

    if sales_df is not None:
        sales_df = sales_df.copy()
        s_sku = find_col(sales_df, ['Item Id', 'item_id', 'Item ID'])
        # Blinkit sales report uses Supply City, not Facility Name
        s_city = find_col(sales_df, ['Supply City', 'Facility Name', 'Store', 'Warehouse', 'City'])
        # Detect actual date span for accurate DOC normalisation
        date_col = find_col(sales_df, ['Order Date', 'OrderDate', 'Date', 'date'])
        if date_col:
            dates = pd.to_datetime(sales_df[date_col], errors='coerce').dropna()
            if not dates.empty:
                n_days = max((dates.max() - dates.min()).days + 1, 1)
        if s_sku and s_city:
            sales_df['c_sku'] = sales_df[s_sku].astype(str).str.strip()
            sales_df['c_city'] = sales_df[s_city].astype(str).str.strip()
            sales_grp = sales_df.groupby(['c_sku', 'c_city'])['Quantity'].sum().reset_index()
            inv_df = inv_df.merge(sales_grp, left_on=['channel_sku', '_city_key'],
                                  right_on=['c_sku', 'c_city'], how='left').fillna(0)
            sales_val = pd.to_numeric(inv_df['Quantity'], errors='coerce').fillna(0)
            has_sales = True

    if has_sales:
        # Sales file: normalise to day span; fall back to Last 30 days for zero-sales locations
        daily_rate = (sales_val / n_days).replace(0, 0.001)
        sales_30d = sales_val * (30 / n_days)
        inv_df['str'] = sales_30d / (sales_30d + inv_df['inventory']).replace(0, 1)
        last30 = pd.to_numeric(inv_df.get('Last 30 days', pd.Series(0, index=inv_df.index)),
                               errors='coerce').fillna(0)
        computed_doc = inv_df['inventory'] / daily_rate
        fallback_doc = inv_df['inventory'] / (last30 / 30).replace(0, 0.001)
        inv_df['doc'] = computed_doc.where(sales_val > 0, fallback_doc)
        # DRR: from sales where available, back-computed from fallback doc otherwise
        # Only back-compute when last30 > 0 — last30=0 means no data, so DRR is unknown (0)
        fallback_drr = (last30 / 30).round(2)
        inv_df['drr'] = (sales_val / n_days).where(sales_val > 0, fallback_drr).round(2)
        inv_df['units_sold'] = sales_val
        inv_df['n_days'] = n_days
    else:
        # No sales file: use Last 30 days column from inventory report directly
        last30 = pd.to_numeric(inv_df.get('Last 30 days', pd.Series(0, index=inv_df.index)),
                               errors='coerce').fillna(0)
        inv_df['str'] = last30 / (last30 + inv_df['inventory']).replace(0, 1)
        inv_df['doc'] = inv_df['inventory'] / (last30 / 30).replace(0, 0.001)
        inv_df['drr'] = (last30 / 30).round(2)
        inv_df['units_sold'] = last30
        inv_df['n_days'] = 30

    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'drr', 'units_sold', 'n_days', 'location']]

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
        # DRR: from sales where available, back-computed from DaysOnHand fallback otherwise
        # Only back-compute when doh > 0 — doh=0 means no data, so DRR is unknown (0)
        fallback_drr = (inv_df['inventory'] / doh_fallback.where(doh_fallback > 0, other=float('nan'))).fillna(0).round(2)
        inv_df['drr'] = (sales_val / n_days).where(sales_val > 0, fallback_drr).round(2)
        inv_df['units_sold'] = sales_val
        inv_df['n_days'] = n_days
    else:
        # No sales file: use Swiggy's own DaysOnHand as DOC, STR unavailable
        inv_df['doc'] = doh_fallback.values
        inv_df['str'] = 0.0
        inv_df['drr'] = (inv_df['inventory'] / doh_fallback.where(doh_fallback > 0, other=float('nan'))).fillna(0).round(2)
        inv_df['units_sold'] = 0.0
        inv_df['n_days'] = 30

    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'drr', 'units_sold', 'n_days', 'location']]

def parse_bigbasket(inv_df, sales_df=None):
    import re
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SKU_Id'].astype(str).str.strip()
    inv_df['location'] = inv_df['DC'].astype(str).str.strip() if 'DC' in inv_df.columns else "Unknown"
    inv_df['inventory'] = pd.to_numeric(inv_df['Total SOH'], errors='coerce').fillna(0)

    # BB inventory has SOH Day of Cover (HO) — pre-computed DOC, reliable fallback
    doh_col = find_col(inv_df, ['SOH Day of Cover (HO)', 'SOH Day of Cover', 'Day of Cover'])
    doh_fallback = pd.to_numeric(inv_df[doh_col], errors='coerce').fillna(0).clip(upper=365) \
        if doh_col else pd.Series(0.0, index=inv_df.index)

    # Map DC name -> BB sales source_city_name
    # Strip '-DC' / '-DC2' / ' DC' suffix, then apply known city aliases
    BB_DC_CITY_MAP = {
        'Ahmedabad':    'Ahmedabad-Gandhinagar',
        'Bhubaneswar':  'Bhubaneshwar-Cuttack',
        'Kundli':       'Gurgaon',
        'Lucknow':      'Lucknow-Kanpur',
        'Vadodara':     'Ahmedabad-Gandhinagar',
        'Vijayawada':   'Vijayawada-Guntur',
    }
    def dc_to_city(dc_name):
        city = re.sub(r'[-\s]?DC\d*$', '', str(dc_name), flags=re.IGNORECASE).strip()
        return BB_DC_CITY_MAP.get(city, city)

    inv_df['_city_key'] = inv_df['location'].apply(dc_to_city)

    n_days = 30  # default
    has_sales = False
    sales_val = pd.Series(0.0, index=inv_df.index)

    if sales_df is not None:
        sales_df = sales_df.copy()
        s_sku = find_col(sales_df, ['source_sku_id', 'SKU_Id', 'SKU ID'])
        s_city = find_col(sales_df, ['source_city_name', 'city_name', 'DC', 'City'])

        # BB sales encodes date range as a string field: '20260301 - 20260328'
        date_range_col = find_col(sales_df, ['date_range', 'Date Range', 'daterange'])
        if date_range_col:
            try:
                dr = sales_df[date_range_col].dropna().iloc[0]
                parts = str(dr).split(' - ')
                d1 = pd.to_datetime(parts[0].strip(), format='%Y%m%d')
                d2 = pd.to_datetime(parts[1].strip(), format='%Y%m%d')
                n_days = max((d2 - d1).days + 1, 1)
            except Exception:
                pass

        if s_sku and s_city:
            sales_df['c_sku'] = sales_df[s_sku].astype(str).str.strip()
            sales_df['c_city'] = sales_df[s_city].astype(str).str.strip()
            sales_grp = sales_df.groupby(['c_sku', 'c_city'])['total_quantity'].sum().reset_index()
            inv_df = inv_df.merge(sales_grp, left_on=['channel_sku', '_city_key'],
                                  right_on=['c_sku', 'c_city'], how='left').fillna(0)
            sales_val = pd.to_numeric(inv_df['total_quantity'], errors='coerce').fillna(0)
            has_sales = True

    if has_sales:
        daily_rate = (sales_val / n_days).replace(0, 0.001)
        sales_30d = sales_val * (30 / n_days)
        inv_df['str'] = sales_30d / (sales_30d + inv_df['inventory']).replace(0, 1)
        computed_doc = inv_df['inventory'] / daily_rate
        # Where no sales matched, fall back to BB's own SOH Day of Cover
        inv_df['doc'] = computed_doc.where(sales_val > 0, doh_fallback.values)
        # DRR: from sales where available, back-computed from DOH fallback otherwise
        # Only back-compute when doh > 0 — doh=0 means no data, so DRR is unknown (0)
        fallback_drr = (inv_df['inventory'] / doh_fallback.where(doh_fallback > 0, other=float('nan'))).fillna(0).round(2)
        inv_df['drr'] = (sales_val / n_days).where(sales_val > 0, fallback_drr).round(2)
        inv_df['units_sold'] = sales_val
        inv_df['n_days'] = n_days
    else:
        # No sales file: use BB's pre-computed SOH Day of Cover directly
        inv_df['str'] = 0.0
        inv_df['doc'] = doh_fallback.values
        inv_df['drr'] = (inv_df['inventory'] / doh_fallback.where(doh_fallback > 0, other=float('nan'))).fillna(0).round(2)
        inv_df['units_sold'] = 0.0
        inv_df['n_days'] = 30

    return inv_df[['channel_sku', 'inventory', 'str', 'doc', 'drr', 'units_sold', 'n_days', 'location']]

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

        # Exclude zero-inventory rows — they have no meaningful DOC/STR and skew metrics
        filtered_df = filtered_df[filtered_df['inventory'] > 0].copy()

        # Location filter — only show locations that have active (non-zero) inventory
        u_locations = sorted(filtered_df['location'].dropna().unique().tolist())
        sel_locations = st.sidebar.multiselect("Filter by Location", u_locations, default=u_locations)
        filtered_df = filtered_df[filtered_df['location'].isin(sel_locations)]

        # --- ACTIONABLE FILTERS (Drive both metrics and table) ---
        st.sidebar.divider()
        st.sidebar.header("🎯 Actionable Filters")
        st.sidebar.caption("Metrics and table both update based on these thresholds.")

        # DOC range — slider + typed number inputs
        st.sidebar.markdown("**Days of Cover (DOC) range**")
        _doc_col1, _doc_col2 = st.sidebar.columns(2)
        _doc_min_box = _doc_col1.number_input(
            "Min days", min_value=0, max_value=9999, value=0, step=1, key="doc_min_box"
        )
        _doc_max_box = _doc_col2.number_input(
            "Max days", min_value=0, max_value=9999, value=9999, step=1, key="doc_max_box"
        )
        _doc_slider = st.sidebar.slider(
            "DOC slider", min_value=0, max_value=9999,
            value=(int(_doc_min_box), int(_doc_max_box)),
            step=1, label_visibility="collapsed"
        )
        # Typed input always wins; fall back to slider when boxes are at default
        if _doc_min_box != 0 or _doc_max_box != 9999:
            min_doc, max_doc = int(_doc_min_box), int(_doc_max_box)
        else:
            min_doc, max_doc = _doc_slider

        # STR range — slider + typed number inputs
        st.sidebar.markdown("**Sell-Through Rate (STR) range %**")
        _str_col1, _str_col2 = st.sidebar.columns(2)
        _str_min_box = _str_col1.number_input(
            "Min %", min_value=0, max_value=200, value=0, step=1, key="str_min_box"
        )
        _str_max_box = _str_col2.number_input(
            "Max %", min_value=0, max_value=200, value=200, step=1, key="str_max_box"
        )
        _str_slider = st.sidebar.slider(
            "STR slider", min_value=0, max_value=200,
            value=(int(_str_min_box), int(_str_max_box)),
            step=1, label_visibility="collapsed"
        )
        if _str_min_box != 0 or _str_max_box != 200:
            min_str, max_str = int(_str_min_box), int(_str_max_box)
        else:
            min_str, max_str = _str_slider

        # --- Apply actionable filters early so metrics reflect them too ---
        action_active = (min_doc > 0 or max_doc < 9999 or min_str > 0 or max_str < 200)
        table_df = filtered_df[
            (filtered_df['doc'] >= min_doc) &
            (filtered_df['doc'] <= max_doc) &
            (filtered_df['str'] >= min_str / 100) &
            (filtered_df['str'] <= max_str / 100)
        ].copy()

        # Metrics always reflect the actionable-filtered view
        metrics_df = table_df

        # Build a dynamic label suffix for metric headers
        doc_label = f" | DOC {min_doc}–{max_doc}d" if (min_doc > 0 or max_doc < 9999) else ""
        str_label = f" | STR {min_str}–{max_str}%" if (min_str > 0 or max_str < 200) else ""
        filter_label = doc_label + str_label

        # --- 4. TOP LINE METRICS (WEIGHTED AVERAGES) ---
        st.divider()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(f"Total Inventory{filter_label}", f"{metrics_df['inventory'].sum():,.0f} units")

        # Weighted avg DOC: also exclude sentinel-inflated rows (no sales data, doc > 9999)
        valid_doc_data = metrics_df[(metrics_df['doc'] > 0) & (metrics_df['doc'] < 9999)]
        if not valid_doc_data.empty and valid_doc_data['inventory'].sum() > 0:
            weighted_doc = (valid_doc_data['doc'] * valid_doc_data['inventory']).sum() / valid_doc_data['inventory'].sum()
            m2.metric(f"Avg Days of Cover{filter_label}", f"{weighted_doc:.1f} days")
        else:
            m2.metric(f"Avg Days of Cover{filter_label}", "N/A")

        # Weighted avg STR across all rows with inventory (already filtered above)
        if not metrics_df.empty:
            weighted_str = (metrics_df['str'] * metrics_df['inventory']).sum() / metrics_df['inventory'].sum()
            m3.metric(f"Avg Sell-Through %{filter_label}", f"{weighted_str:.2%}")
        else:
            m3.metric(f"Avg Sell-Through %{filter_label}", "0.00%")

        # Avg DRR = total units sold across period / n_days
        # Each channel may have a different n_days, so compute per-channel then sum
        if not metrics_df.empty and metrics_df['units_sold'].sum() > 0:
            total_units = 0
            total_days = 0
            for ch, grp in metrics_df.groupby('channel'):
                ch_units = grp['units_sold'].sum()
                ch_days = grp['n_days'].max()
                if ch_days > 0:
                    total_units += ch_units
                    total_days = max(total_days, ch_days)
            avg_drr = total_units / total_days if total_days > 0 else 0
            m4.metric(f"Avg DRR{filter_label}", f"{avg_drr:.2f} units/day")
        else:
            m4.metric(f"Avg DRR{filter_label}", "N/A")

        # --- 5. DASHBOARD TABLE ---
        st.subheader("📊 Inventory Performance by Location")

        # Filter summary caption
        total_rows = len(filtered_df)
        shown_rows = len(table_df)
        if action_active:
            filter_parts = []
            if min_doc > 0 or max_doc < 9999:
                filter_parts.append(f"DOC **{min_doc}–{max_doc}** days")
            if min_str > 0 or max_str < 200:
                filter_parts.append(f"STR **{min_str}–{max_str}%**")
            st.caption(
                f"🎯 Showing **{shown_rows}** of {total_rows} rows — filtered by {', '.join(filter_parts)}"
            )
        else:
            st.caption(f"Showing all **{total_rows}** rows. Use the Actionable Filters in the sidebar to narrow down.")

        # --- Group-by radio ---
        group_by = st.radio(
            "Group by:",
            options=["None", "Channel", "Product", "Location"],
            horizontal=True,
            index=0,
            help="Aggregate the table rows by the selected dimension"
        )

        def color_doc(val):
            if val < 7:
                return 'color: red; font-weight: bold'
            elif val < 15:
                return 'color: orange; font-weight: bold'
            else:
                return ''

        display_cols = ['master_sku', 'channel_sku', 'channel', 'location', 'inventory', 'drr', 'doc', 'str']

        if group_by == "None":
            render_df = table_df[display_cols].sort_values('inventory', ascending=False)
            st.dataframe(
                render_df.style.format({
                    'str': '{:.2%}', 'doc': '{:.1f}', 'inventory': '{:,.0f}', 'drr': '{:.2f}', 'units_sold': '{:,.1f}'
                }).applymap(color_doc, subset=['doc']),
                use_container_width=True
            )
        else:
            # Build aggregation by the chosen dimension
            group_col_map = {"Channel": "channel", "Product": "master_sku", "Location": "location"}
            grp_col = group_col_map[group_by]

            agg_df = table_df.groupby(grp_col).agg(
                inventory=('inventory', 'sum'),
                units_sold=('units_sold', 'sum'),
            ).reset_index()

            # Weighted avg DOC per group (inventory-weighted, exclude sentinel rows)
            def weighted_doc(grp):
                valid = grp[(grp['doc'] > 0) & (grp['doc'] < 9999)]
                if valid.empty or valid['inventory'].sum() == 0:
                    return float('nan')
                return (valid['doc'] * valid['inventory']).sum() / valid['inventory'].sum()

            def weighted_str(grp):
                if grp['inventory'].sum() == 0:
                    return 0.0
                return (grp['str'] * grp['inventory']).sum() / grp['inventory'].sum()

            def group_drr(grp):
                total_u = 0; total_d = 0
                for ch, sub in grp.groupby('channel'):
                    total_u += sub['units_sold'].sum()
                    total_d = max(total_d, sub['n_days'].max())
                return total_u / total_d if total_d > 0 else 0.0

            doc_series = table_df.groupby(grp_col).apply(weighted_doc).rename('doc')
            str_series = table_df.groupby(grp_col).apply(weighted_str).rename('str')
            drr_series = table_df.groupby(grp_col).apply(group_drr).rename('drr')

            agg_df = agg_df.join(doc_series, on=grp_col)
            agg_df = agg_df.join(str_series, on=grp_col)
            agg_df = agg_df.join(drr_series, on=grp_col)
            agg_df = agg_df.sort_values('inventory', ascending=False).reset_index(drop=True)

            st.dataframe(
                agg_df.style.format({
                    'str': '{:.2%}', 'doc': '{:.1f}', 'inventory': '{:,.0f}', 'drr': '{:.2f}', 'units_sold': '{:,.1f}'
                }).applymap(color_doc, subset=['doc']),
                use_container_width=True
            )

        # --- 6. ACTIONABLE QUADRANT TABS ---
        st.divider()
        st.subheader("🎯 Actionable Quadrants")

        # Thresholds derived from actual data distribution (p50 DOC ≈ 90d, p50 STR ≈ 20%)
        DOC_THRESH = 90   # days — below this = Low DOC (running out)
        STR_THRESH = 20   # % — below this = Low STR (slow mover)

        # Work from the full filtered_df (not table_df) so quadrants reflect all inventory,
        # unaffected by the actionable sliders — gives complete picture per quadrant
        quad_df = filtered_df[(filtered_df['doc'] > 0) & (filtered_df['doc'] < 9999)].copy()
        quad_df['str_pct'] = quad_df['str'] * 100

        q1 = quad_df[(quad_df['str_pct'] >= STR_THRESH) & (quad_df['doc'] <  DOC_THRESH)].sort_values('doc')
        q2 = quad_df[(quad_df['str_pct'] <  STR_THRESH) & (quad_df['doc'] >= DOC_THRESH)].sort_values('doc', ascending=False)
        q3 = quad_df[(quad_df['str_pct'] >= STR_THRESH) & (quad_df['doc'] >= DOC_THRESH)].sort_values('doc', ascending=False)
        q4 = quad_df[(quad_df['str_pct'] <  STR_THRESH) & (quad_df['doc'] <  DOC_THRESH)].sort_values('doc')

        tab_labels = [
            f"🔴 Reorder Now ({len(q1)})",
            f"🟡 Run Promotion ({len(q2)})",
            f"🟢 Improve Visibility ({len(q3)})",
            f"⚫ SKU Rationalise ({len(q4)})",
        ]
        t1, t2, t3, t4 = st.tabs(tab_labels)

        quad_display_cols = ['master_sku', 'channel_sku', 'channel', 'location', 'inventory', 'drr', 'doc', 'str']
        quad_fmt = {'str': '{:.2%}', 'doc': '{:.1f}', 'inventory': '{:,.0f}', 'drr': '{:.2f}'}

        def download_csv(df, cols, label, filename):
            """Render a download button for a quadrant dataframe."""
            export = df[cols].copy()
            export['str'] = (export['str'] * 100).round(2).astype(str) + '%'
            export['doc'] = export['doc'].round(1)
            export['drr'] = export['drr'].round(2)
            csv_bytes = export.to_csv(index=False).encode('utf-8')
            st.download_button(
                label=f"⬇️ Download {label} as CSV",
                data=csv_bytes,
                file_name=filename,
                mime='text/csv',
                use_container_width=True,
            )

        def quad_summary(df, inv_col='inventory', doc_col='doc', str_col='str_pct'):
            n = len(df)
            total_inv = df[inv_col].sum()
            avg_doc = (df[doc_col] * df[inv_col]).sum() / df[inv_col].sum() if total_inv > 0 else 0
            avg_str = df[str_col].mean()
            return n, total_inv, avg_doc, avg_str

        with t1:
            st.markdown(
                "**High STR + Low DOC** — Hot sellers running out fast. "
                f"Threshold: STR ≥ {STR_THRESH}%, DOC < {DOC_THRESH} days."
            )
            st.caption("Action: **Reorder immediately**; consider increasing the next order size.")
            if q1.empty:
                st.success("✅ No SKUs in this quadrant.")
            else:
                n, inv, doc, str_ = quad_summary(q1)
                c1, c2, c3 = st.columns(3)
                c1.metric("SKU-locations", n)
                c2.metric("Total Inventory", f"{inv:,.0f} units")
                c3.metric("Avg DOC", f"{doc:.1f} days")
                st.dataframe(
                    q1[quad_display_cols].reset_index(drop=True)
                    .style.format(quad_fmt).applymap(color_doc, subset=['doc']),
                    use_container_width=True
                )
                download_csv(q1, quad_display_cols, "Reorder Now", "reorder_now.csv")

        with t2:
            st.markdown(
                "**Low STR + High DOC** — Slow movers; overstocked. "
                f"Threshold: STR < {STR_THRESH}%, DOC ≥ {DOC_THRESH} days."
            )
            st.caption("Action: **Run a promotion**, bundle the item, or stop future orders.")
            if q2.empty:
                st.success("✅ No SKUs in this quadrant.")
            else:
                n, inv, doc, str_ = quad_summary(q2)
                c1, c2, c3 = st.columns(3)
                c1.metric("SKU-locations", n)
                c2.metric("Total Inventory", f"{inv:,.0f} units")
                c3.metric("Avg DOC", f"{doc:.1f} days")
                st.dataframe(
                    q2[quad_display_cols].reset_index(drop=True)
                    .style.format(quad_fmt).applymap(color_doc, subset=['doc']),
                    use_container_width=True
                )
                download_csv(q2, quad_display_cols, "Run Promotion", "run_promotion.csv")

        with t3:
            st.markdown(
                "**High STR + High DOC** — High demand but massive oversupply. "
                f"Threshold: STR ≥ {STR_THRESH}%, DOC ≥ {DOC_THRESH} days."
            )
            st.caption("Action: **Improve visibility/merchandising** to maintain the high sales pace.")
            if q3.empty:
                st.success("✅ No SKUs in this quadrant.")
            else:
                n, inv, doc, str_ = quad_summary(q3)
                c1, c2, c3 = st.columns(3)
                c1.metric("SKU-locations", n)
                c2.metric("Total Inventory", f"{inv:,.0f} units")
                c3.metric("Avg DOC", f"{doc:.1f} days")
                st.dataframe(
                    q3[quad_display_cols].reset_index(drop=True)
                    .style.format(quad_fmt).applymap(color_doc, subset=['doc']),
                    use_container_width=True
                )
                download_csv(q3, quad_display_cols, "Improve Visibility", "improve_visibility.csv")

        with t4:
            st.markdown(
                "**Low STR + Low DOC** — Poor demand and low stock. "
                f"Threshold: STR < {STR_THRESH}%, DOC < {DOC_THRESH} days."
            )
            st.caption("Action: Likely a candidate for **SKU rationalisation** (discontinuing the item).")
            if q4.empty:
                st.success("✅ No SKUs in this quadrant.")
            else:
                n, inv, doc, str_ = quad_summary(q4)
                c1, c2, c3 = st.columns(3)
                c1.metric("SKU-locations", n)
                c2.metric("Total Inventory", f"{inv:,.0f} units")
                c3.metric("Avg DOC", f"{doc:.1f} days")
                st.dataframe(
                    q4[quad_display_cols].reset_index(drop=True)
                    .style.format(quad_fmt).applymap(color_doc, subset=['doc']),
                    use_container_width=True
                )
                download_csv(q4, quad_display_cols, "SKU Rationalise", "sku_rationalise.csv")

else:
    st.info("Upload channel files to generate the dashboard.")
