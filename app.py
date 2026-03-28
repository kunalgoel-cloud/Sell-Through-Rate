import streamlit as st
import pandas as pd
import os

# --- CONFIG & PERSISTENCE ---
st.set_page_config(page_title="Mama Nourish Inventory Engine", layout="wide")
MAPPING_FILE = "mapping_db.csv"
COMMON_SKU_FILE = "2026-03-28T16-00_export.csv"

# Initialize local mapping database if it doesn't exist
if not os.path.exists(MAPPING_FILE):
    pd.DataFrame(columns=["channel", "channel_sku", "master_sku"]).to_csv(MAPPING_FILE, index=False)

def load_master_skus():
    try:
        df = pd.read_csv(COMMON_SKU_FILE)
        return sorted(df['name'].unique().tolist())
    except:
        return []

def load_mapping():
    return pd.read_csv(MAPPING_FILE).astype(str)

def save_new_mappings(new_entries):
    current_df = load_mapping()
    new_df = pd.DataFrame(new_entries)
    updated_df = pd.concat([current_df, new_df], ignore_index=True).drop_duplicates(subset=['channel', 'channel_sku'])
    updated_df.to_csv(MAPPING_FILE, index=False)

# --- CHANNEL PARSERS ---

def parse_amazon(df):
    df = df.copy()
    df['channel_sku'] = df['ASIN'].astype(str).str.strip()
    df['inventory'] = pd.to_numeric(df['Sellable On Hand Units'], errors='coerce').fillna(0)
    # Amazon provides STR directly
    df['str'] = pd.to_numeric(df['Sell-Through %'].astype(str).str.replace('%',''), errors='coerce').fillna(0) / 100
    df['doc'] = 0 # Amazon logic usually uses STR
    return df[['channel_sku', 'inventory', 'str', 'doc']]

def parse_blinkit(df):
    df = df.copy()
    df['channel_sku'] = df['Item ID'].astype(str).str.strip()
    df['inventory'] = pd.to_numeric(df['Total sellable'], errors='coerce').fillna(0)
    drr = pd.to_numeric(df['Last 30 days'], errors='coerce').fillna(0) / 30
    df['doc'] = df['inventory'] / drr.replace(0, 0.001)
    df['str'] = 0
    return df[['channel_sku', 'inventory', 'doc', 'str']]

def parse_swiggy(inv_df, sales_df):
    drr = sales_df.groupby('ITEM_CODE')['UNITS_SOLD'].sum() / 30
    inv_df = inv_df.copy()
    inv_df['channel_sku'] = inv_df['SkuCode'].astype(str).str.strip()
    inv_df = inv_df.merge(drr, left_on='SkuCode', right_index=True, how='left').fillna(0)
    inv_df['inventory'] = inv_df['WarehouseQtyAvailable']
    inv_df['doc'] = inv_df['inventory'] / inv_df['UNITS_SOLD'].replace(0, 0.001)
    inv_df['str'] = 0
    return inv_df[['channel_sku', 'inventory', 'doc', 'str']]

def parse_bigbasket(df):
    df = df.copy()
    df['channel_sku'] = df['SKU_Id'].astype(str).str.strip()
    df['inventory'] = pd.to_numeric(df['Total SOH'], errors='coerce').fillna(0)
    df['doc'] = pd.to_numeric(df['SOH Day of Cover (HO)'], errors='coerce').fillna(0)
    df['str'] = 0
    return df[['channel_sku', 'inventory', 'doc', 'str']]

# --- APP UI ---

st.title("🛡️ Mama Nourish Inventory Control")

MASTER_SKU_LIST = load_master_skus()
MAPPING_DB = load_mapping()

# 1. FILE UPLOADS
st.subheader("📥 Upload Channel Files")
cols = st.columns(4)
uploaded_data = []

with cols[0]:
    amz = st.file_uploader("Amazon", type="csv")
    if amz:
        uploaded_data.append((parse_amazon(pd.read_csv(amz, skiprows=1)), 'Amazon'))

with cols[1]:
    blk = st.file_uploader("Blinkit", type="csv")
    if blk:
        uploaded_data.append((parse_blinkit(pd.read_csv(blk, skiprows=2)), 'Blinkit'))

with cols[2]:
    swg_i = st.file_uploader("Swiggy Inv", type="csv")
    swg_s = st.file_uploader("Swiggy Sales", type="csv")
    if swg_i and swg_s:
        uploaded_data.append((parse_swiggy(pd.read_csv(swg_i), pd.read_csv(swg_s)), 'Swiggy'))

with cols[3]:
    bb = st.file_uploader("Big Basket", type="csv")
    if bb:
        uploaded_data.append((parse_bigbasket(pd.read_csv(bb)), 'Big Basket'))

# 2. PROCESSING & MAPPING
if uploaded_data:
    raw_dfs = []
    for df, channel in uploaded_data:
        df['channel'] = channel
        raw_dfs.append(df)
    
    combined_raw = pd.concat(raw_dfs, ignore_index=True)
    
    # Merge with existing mapping
    merged = combined_raw.merge(MAPPING_DB, on=['channel', 'channel_sku'], how='left')
    
    # Check for unmapped items
    unmapped = merged[merged['master_sku'].isna()][['channel', 'channel_sku']].drop_duplicates()
    
    if not unmapped.empty:
        st.warning(f"🚨 {len(unmapped)} New SKUs detected. Map them below to continue.")
        with st.form("mapping_wizard"):
            new_mapping_entries = []
            for _, row in unmapped.iterrows():
                chan, c_sku = row['channel'], row['channel_sku']
                choice = st.selectbox(f"Map {chan} ID: {c_sku}", ["Select..."] + MASTER_SKU_LIST, key=f"{chan}_{c_sku}")
                if choice != "Select...":
                    new_mapping_entries.append({"channel": chan, "channel_sku": c_sku, "master_sku": choice})
            
            if st.form_submit_button("Save Mappings & Update Dashboard"):
                if new_mapping_entries:
                    save_new_mappings(new_mapping_entries)
                    st.rerun()
    else:
        # 3. FINAL DASHBOARD
        st.divider()
        st.subheader("📊 Consolidated Inventory Health")
        
        # UI Table with Risk Colors
        def highlight_risks(val):
            if isinstance(val, float) and 0 < val < 15:
                return 'background-color: #ff4b4b; color: white'
            return ''

        st.dataframe(
            merged[['master_sku', 'channel', 'inventory', 'doc', 'str']].style.applymap(highlight_risks, subset=['doc']),
            use_container_width=True
        )
else:
    st.info("Upload channel files to generate the report.")
