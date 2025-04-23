# type: ignore
import streamlit as st
import pandas as pd
import json
import tempfile
import os
from db_utils import DBManager
from agent import QueryAgent

# Set page configuration
st.set_page_config(
    page_title="DB Assistant Chatbot",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed"  # Start with sidebar collapsed for more chat space
)

# Function to process database operations based on parsed query
def process_operation(query_result):
    """Process the database operation based on the parsed query result"""
    operation = query_result["operation"]
    target = query_result.get("target", "")
    parameters = query_result.get("parameters", {})
    
    db_manager = st.session_state.db_manager
    
    # PostgreSQL Operations
    if operation == "list_tables":
        return db_manager.postgres_list_tables()
    
    elif operation == "view_table":
        limit = parameters.get("limit", 100)
        return db_manager.postgres_view_table(target, limit)
    
    elif operation == "count_records":
        return db_manager.postgres_count_records(target)
    
    elif operation == "add_record":
        record_data = parameters.get("data", {})
        return db_manager.postgres_add_record(target, record_data)
    
    elif operation == "delete_record":
        condition = parameters.get("condition", "")
        return db_manager.postgres_delete_record(target, condition)
    
    elif operation == "create_table_from_csv":
        # This is handled by the UI file uploader
        return False, "Please use the file uploader to create a table from CSV."
    
    # New PostgreSQL operations
    elif operation == "create_table":
        columns = parameters.get("columns", {})
        return db_manager.postgres_create_table(target, columns)
    
    elif operation == "add_column":
        column_name = parameters.get("column_name", "")
        column_type = parameters.get("column_type", "TEXT")
        return db_manager.postgres_add_column(target, column_name, column_type)
    
    elif operation == "delete_column":
        column_name = parameters.get("column_name", "")
        return db_manager.postgres_delete_column(target, column_name)
    
    elif operation == "rename_column":
        old_name = parameters.get("old_name", "")
        new_name = parameters.get("new_name", "")
        return db_manager.postgres_rename_column(target, old_name, new_name)
    
    elif operation == "rename_table":
        new_name = parameters.get("new_name", "")
        return db_manager.postgres_rename_table(target, new_name)
    
    elif operation == "update_row":
        set_values = parameters.get("set_values", {})
        condition = parameters.get("condition", "")
        return db_manager.postgres_update_row(target, set_values, condition)
    
    elif operation == "run_query":
        query = parameters.get("query", "")
        params = parameters.get("params", None)
        return db_manager.postgres_run_query(query, params)
    
    # MongoDB Operations
    elif operation == "list_collections":
        return db_manager.mongo_list_collections()
    
    elif operation == "view_collection":
        limit = parameters.get("limit", 100)
        return db_manager.mongo_view_collection(target, limit)
    
    elif operation == "count_documents":
        filter_query = parameters.get("filter", {})
        return db_manager.mongo_count_documents(target, filter_query)
    
    elif operation == "add_document":
        document_data = parameters.get("data", {})
        return db_manager.mongo_add_document(target, document_data)
    
    elif operation == "delete_document":
        filter_query = parameters.get("filter", {})
        return db_manager.mongo_delete_document(target, filter_query)
    
    elif operation == "create_collection_from_csv":
        # This is handled by the UI file uploader
        return False, "Please use the file uploader to create a collection from CSV."
    
    # New MongoDB operations
    elif operation == "create_collection":
        return db_manager.mongo_create_collection(target)
    
    elif operation == "rename_collection":
        new_name = parameters.get("new_name", "")
        return db_manager.mongo_rename_collection(target, new_name)
    
    elif operation == "update_document":
        filter_query = parameters.get("filter", {})
        update_data = parameters.get("update", {})
        return db_manager.mongo_update_document(target, filter_query, update_data)
    
    elif operation == "run_aggregation":
        pipeline = parameters.get("pipeline", [])
        return db_manager.mongo_run_aggregation(target, pipeline)
    
    elif operation == "add_multiple_columns":
        columns_data = parameters.get("columns_data", {})
        return db_manager.postgres_add_multiple_columns(target, columns_data)
    
    else:
        return False, f"Operation '{operation}' not implemented or recognized."

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "db_manager" not in st.session_state:
    st.session_state.db_manager = DBManager()
if "query_agent" not in st.session_state:
    st.session_state.query_agent = QueryAgent()
if "show_upload" not in st.session_state:
    st.session_state.show_upload = False  # Control upload section visibility

# Main layout
st.title("🤖 DB Assistant Chatbot")
st.write("Interact with PostgreSQL and MongoDB databases using natural language. Connect to your databases in the sidebar, then start asking questions below.")
    
# Toggle for CSV uploader visibility
show_uploader = st.checkbox("Show CSV upload section", value=st.session_state.show_upload)
st.session_state.show_upload = show_uploader

# Add initial welcome message if chat is empty
if len(st.session_state.messages) == 0:
    st.session_state.messages.append({
        "role": "assistant", 
        "content": "Hello! I'm your database assistant. How can I help you with your PostgreSQL or MongoDB databases today?"
    })

# Display chat messages using Streamlit's native chat elements
for message in st.session_state.messages:
    if message["role"] == "user":
        with st.chat_message("user"):
            st.write(message["content"])
    else:
        with st.chat_message("assistant"):
            if isinstance(message["content"], pd.DataFrame):
                st.write("Here's the data you requested:")
                st.dataframe(message["content"], height=min(400, len(message["content"]) * 35 + 38))
            else:
                st.write(message["content"])

# Chat input
user_input = st.chat_input("Ask me about your databases...")

# CSV upload section - only shown when toggle is active
if st.session_state.show_upload:
    st.markdown("---")
    st.subheader("Upload CSV Data")
    uploaded_file = st.file_uploader("Upload a CSV file to create a new table/collection", type="csv")
    
    if uploaded_file is not None:
        # Preview the CSV in a more compact way
        try:
            df_preview = pd.read_csv(uploaded_file)
            with st.expander("CSV Preview", expanded=True):
                st.dataframe(df_preview.head(), height=200)
            
            # Allow user to choose target database and provide a name
            cols = st.columns(2)
            with cols[0]:
                target_db = st.radio("Select target database:", ["PostgreSQL", "MongoDB"])
            with cols[1]:
                target_name = st.text_input("Table/Collection name:")
            
            if st.button("Create from CSV") and target_name:
                # Save uploaded file to a temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    temp_file_path = tmp_file.name
                
                try:
                    # Process based on selected database
                    if target_db == "PostgreSQL":
                        status, message = st.session_state.db_manager.postgres_create_table_from_csv(
                            target_name, temp_file_path
                        )
                    else:  # MongoDB
                        status, message = st.session_state.db_manager.mongo_create_collection_from_csv(
                            target_name, temp_file_path
                        )
                    
                    # Add system message with result
                    st.session_state.messages.append({"role": "assistant", "content": message})
                    st.rerun()
                except Exception as e:
                    st.error(f"Error creating from CSV: {str(e)}")
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
        except Exception as e:
            st.error(f"Error reading CSV file: {str(e)}")

# Streamlined sidebar for database connection controls
with st.sidebar:
    st.header("Database Connections")
    
    # PostgreSQL connection section
    with st.expander("PostgreSQL Settings", expanded=True):
        pg_host = st.text_input("Host", "localhost", key="pg_host")
        cols = st.columns(2)
        with cols[0]:
            pg_port = st.text_input("Port", "5432", key="pg_port")
        with cols[1]:
            pg_db = st.text_input("Database", "postgres", key="pg_db")
        pg_user = st.text_input("Username", "postgres", key="pg_user")
        pg_password = st.text_input("Password", "postgres", key="pg_password", type="password")
        
        if st.button("Connect to PostgreSQL", use_container_width=True):
            # Update connection details
            st.session_state.db_manager.pg_conn = None  # Close existing connection
            try:
                from config import POSTGRES_CONFIG
                POSTGRES_CONFIG["host"] = pg_host
                POSTGRES_CONFIG["port"] = pg_port
                POSTGRES_CONFIG["database"] = pg_db
                POSTGRES_CONFIG["user"] = pg_user
                POSTGRES_CONFIG["password"] = pg_password
                
                # Try to connect
                status, message = st.session_state.db_manager.connect_postgres()
                if status:
                    st.success(message)
                    # Add a message to the chat
                    st.session_state.messages.append({"role": "assistant", "content": f"✅ {message}"})
                    st.rerun()
                else:
                    st.error(message)
                    # Add an error message to the chat
                    st.session_state.messages.append({"role": "assistant", "content": f"❌ {message}"})
                    st.rerun()
            except Exception as e:
                st.error(f"Error updating PostgreSQL connection: {str(e)}")
    
    # MongoDB connection section
    with st.expander("MongoDB Settings", expanded=True):
        mongo_uri = st.text_input("Connection URI", "mongodb://localhost:27017/", key="mongo_uri")
        mongo_db = st.text_input("Database", "test", key="mongo_db")
        
        if st.button("Connect to MongoDB", use_container_width=True):
            # Update connection details
            st.session_state.db_manager.mongo_client = None  # Close existing connection
            try:
                from config import MONGO_CONFIG
                MONGO_CONFIG["connection_string"] = mongo_uri
                MONGO_CONFIG["database"] = mongo_db
                
                # Try to connect
                status, message = st.session_state.db_manager.connect_mongo()
                if status:
                    st.success(message)
                    # Add a message to the chat
                    st.session_state.messages.append({"role": "assistant", "content": f"✅ {message}"})
                    st.rerun()
                else:
                    st.error(message)
                    # Add an error message to the chat
                    st.session_state.messages.append({"role": "assistant", "content": f"❌ {message}"})
                    st.rerun()
            except Exception as e:
                st.error(f"Error updating MongoDB connection: {str(e)}")
    
    # Clear chat history button
    st.markdown("---")
    if st.button("Clear Chat History", use_container_width=True):
        st.session_state.messages = []
        # Add back the welcome message
        st.session_state.messages.append({
            "role": "assistant", 
            "content": "Hello! I'm your database assistant. How can I help you with your PostgreSQL or MongoDB databases today?"
        })
        st.rerun()

    # Help section
    st.markdown("---")
    with st.expander("Quick Help & Examples", expanded=False):
        st.markdown("""
        **Basic Operations:**
        - "Show all tables in Postgres"
        - "List MongoDB collections"
        - "Count records in users table"
        
        **Create & Modify:**
        - "Create a new table called employees with columns name (text), age (int), and salary (float)"
        - "Add a new product with name='Phone' and price=599"
        - "Update price to 499 for product where id=5"
        
        **Schema Changes:**
        - "Add a status column to orders table"
        - "Rename the users table to customers"
        - "Delete the age column from users table"
        
        **Advanced Queries:**
        - "Show me customers who ordered more than 3 items"
        - "Calculate the average price of products"
        - "Run SQL: SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"
        """)

# Process user input
if user_input:
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": user_input})
    
    # Process the query using the agent
    with st.spinner("Thinking..."):
        # Check if it's an upload-related query, which we handle via the UI
        if st.session_state.query_agent.is_upload_query(user_input):
            response = "To upload a CSV file, please use the file uploader section. I've turned it on for you."
            st.session_state.show_upload = True
        else:
            # Parse the query using the LLM agent
            query_result = st.session_state.query_agent.parse_query(user_input)
            
            if query_result["operation"] == "unknown" or query_result["operation"] == "error":
                # Handle unknown or error operations
                response = query_result["explanation"]
            else:
                # Process known operations
                try:
                    db_result = process_operation(query_result)
                    response = st.session_state.query_agent.format_response(db_result)
                except Exception as e:
                    response = f"Error executing operation: {str(e)}"
    
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})
    
    # Rerun to update the display
    st.rerun()

if __name__ == "__main__":
    # This will be executed when the app is run
    pass 