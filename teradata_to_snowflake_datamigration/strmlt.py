import streamlit as st
import pandas as pd
import datetime

# Set page title and layout
st.set_page_config(page_title="Teradata to Snowflake Migration", layout="wide")

# Initialize session state to hold user inputs
if 'user_inputs' not in st.session_state:
    st.session_state.user_inputs = []

st.title("Teradata to Snowflake Migration Utility")

# Initialize the 4 tabs
tab1, tab3,tab2,    tab4 = st.tabs(["🚀 Data Input", "⚙️ User Inputs", "📋 Config Table", "📜 Logs Table"])

# --- TAB 1: Data Input ---
with tab1:
    st.header("Migration Parameters")
    st.write("Enter the details for the tables you wish to migrate from Teradata to Snowflake.")
    
    # Using a form so the page doesn't rerun on every single keystroke
    with st.form("migration_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Source (Teradata)")
            td_database = st.text_input("Teradata Database")
            td_table = st.text_input("Teradata Table Name")
            
        with col2:
            st.subheader("Target (Snowflake)")
            sf_schema = st.text_input("Snowflake Schema")
            sf_table = st.text_input("Snowflake Table Name")
        
        st.divider()
        migration_mode = st.selectbox("Migration Mode", ["Full Load", "Incremental", "Filter"])
        
        # Submit button for the form
        submitted = st.form_submit_button("Insert to Config Table")
        
        if submitted:
            if td_database and td_table and sf_schema and sf_table:
                # Save the input data to session state
                st.session_state.user_inputs.append({
                    "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "TD Database": td_database,
                    "TD Table": td_table,
                    "SF Schema": sf_schema,
                    "SF Table": sf_table,
                    "Mode": migration_mode
                })
                st.success(f"Migration job started: {td_database}.{td_table} ➡️ {sf_schema}.{sf_table} ({migration_mode})")
            else:
                st.error("Please fill in all database and table fields before starting the migration.")

# --- TAB 2: Configuration Table ---
with tab2:
    st.header("Input Config Table")
    st.write("Current mappings and connection parameters for the migration engine.")
    
    # Creating a mock configuration dataframe
    config_data = {
        "Parameter": ["TD_HOST", "TD_USER", "SF_ACCOUNT", "SF_ROLE", "SF_WAREHOUSE", "BATCH_SIZE", "MAX_PARALLEL_WORKERS"],
        "Value": ["tdprod.internal.corp", "svc_migration_user", "xy12345.us-east-1", "SYSADMIN", "WH_MIGRATION_L", "50000", "8"],
        "Environment": ["Production", "Production", "Production", "Production", "Production", "Global", "Global"]
    }
    df_config = pd.DataFrame(config_data)
    
    # Display the dataframe as an interactive table
    st.dataframe(df_config, use_container_width=True, hide_index=True)
    if st.button("Refresh Log"):
        st.rerun()

# --- TAB 3: User Inputs (NEW TAB) ---
with tab3:
    st.header("Submitted Migration Jobs")
    st.write("A historical record of all migration inputs submitted during this session.")
    
    # Check if there is anything in our session state list
    if st.session_state.user_inputs:
        # Convert the session state list of dictionaries into a dataframe
        df_inputs = pd.DataFrame(st.session_state.user_inputs)
        st.dataframe(df_inputs, use_container_width=True, hide_index=True)
    else:
        st.info("No migration jobs have been submitted yet. Please submit a job in the Data Input tab.")

# --- TAB 4: System Logs ---
with tab4:
    st.header("Migration Logs")
    st.write("Monitor real-time logs for schema conversion, extraction, and loading processes.")
    
    # Generating mock logs with current timestamps
    current_time = datetime.datetime.now()
    mock_logs = f"""
    [{(current_time - datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")}] INFO: Migration utility started.
    [{(current_time - datetime.timedelta(minutes=4)).strftime("%Y-%m-%d %H:%M:%S")}] INFO: Loaded configuration parameters.
    [{(current_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")}] INFO: Connected to Snowflake account (xy12345.us-east-1) successfully.
    [{(current_time - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")}] INFO: Connecting to Teradata host (tdprod.internal.corp)...
    [{current_time.strftime("%Y-%m-%d %H:%M:%S")}] INFO: Ready for migration tasks. Waiting for user input.
    """
    
    # Display logs in a text area
    st.text_area("Terminal Output", value=mock_logs.strip(), height=300, disabled=True)
    
    # Button to simulate refreshing the logs
    if st.button("Refresh Logs"):
        st.rerun()