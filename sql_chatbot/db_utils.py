# type: ignore
import psycopg2
import pandas as pd
from pymongo import MongoClient
from config import POSTGRES_CONFIG, MONGO_CONFIG
import io
import os
import json
from bson import json_util

class DBManager:
    def __init__(self):
        self.pg_conn = None
        self.mongo_client = None
        self.mongo_db = None
    
    # PostgreSQL Connection Methods
    def connect_postgres(self):
        """Connect to PostgreSQL database"""
        try:
            self.pg_conn = psycopg2.connect(
                host=POSTGRES_CONFIG["host"],
                port=POSTGRES_CONFIG["port"],
                database=POSTGRES_CONFIG["database"],
                user=POSTGRES_CONFIG["user"],
                password=POSTGRES_CONFIG["password"]
            )
            return True, "Connected to PostgreSQL"
        except Exception as e:
            return False, f"PostgreSQL connection error: {str(e)}"
    
    def close_postgres(self):
        """Close PostgreSQL connection"""
        if self.pg_conn:
            self.pg_conn.close()
            self.pg_conn = None
            return True, "PostgreSQL connection closed"
        return False, "No active PostgreSQL connection"
    
    # MongoDB Connection Methods
    def connect_mongo(self):
        """Connect to MongoDB database"""
        try:
            self.mongo_client = MongoClient(MONGO_CONFIG["connection_string"])
            self.mongo_db = self.mongo_client[MONGO_CONFIG["database"]]
            return True, "Connected to MongoDB"
        except Exception as e:
            return False, f"MongoDB connection error: {str(e)}"
    
    def close_mongo(self):
        """Close MongoDB connection"""
        if self.mongo_client is not None:
            self.mongo_client.close()
            self.mongo_client = None
            self.mongo_db = None
            return True, "MongoDB connection closed"
        return False, "No active MongoDB connection"
    
    # PostgreSQL Operations
    def postgres_list_tables(self):
        """List all tables in PostgreSQL database"""
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [table[0] for table in cursor.fetchall()]
            cursor.close()
            return True, tables
        except Exception as e:
            return False, f"Error listing PostgreSQL tables: {str(e)}"
    
    def postgres_view_table(self, table_name, limit=100):
        """View contents of a specific PostgreSQL table"""
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            df = pd.read_sql_query(query, self.pg_conn)
            return True, df
        except Exception as e:
            return False, f"Error viewing PostgreSQL table: {str(e)}"
    
    def postgres_count_records(self, table_name):
        """Count records in a PostgreSQL table"""
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return True, count
        except Exception as e:
            return False, f"Error counting records in PostgreSQL table: {str(e)}"
    
    def postgres_add_record(self, table_name, record_data):
        """Add a record to a PostgreSQL table"""
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            columns = ", ".join(record_data.keys())
            placeholders = ", ".join(["%s"] * len(record_data))
            values = list(record_data.values())
            
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(query, values)
            self.pg_conn.commit()
            cursor.close()
            return True, "Record added successfully"
        except Exception as e:
            return False, f"Error adding record to PostgreSQL table: {str(e)}"
    
    def postgres_delete_record(self, table_name, condition):
        """Delete records from a PostgreSQL table based on a condition"""
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            query = f"DELETE FROM {table_name} WHERE {condition}"
            cursor.execute(query)
            deleted_count = cursor.rowcount
            self.pg_conn.commit()
            cursor.close()
            return True, f"{deleted_count} records deleted"
        except Exception as e:
            return False, f"Error deleting records from PostgreSQL table: {str(e)}"
    
    def postgres_create_table_from_csv(self, table_name, csv_file):
        """Create a new PostgreSQL table from a CSV file"""
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Generate CREATE TABLE statement with improved type detection
            columns = []
            for col in df.columns:
                # Use pandas dtype for initial inference
                dtype = df[col].dtype
                
                # Check for all integer values
                if pd.api.types.is_integer_dtype(dtype):
                    # Check if it might be a boolean (only 0s and 1s)
                    unique_vals = set(df[col].dropna().unique())
                    if unique_vals.issubset({0, 1}) and len(unique_vals) <= 2:
                        col_type = "BOOLEAN"
                    else:
                        col_type = "INTEGER"
                
                # Check for float values
                elif pd.api.types.is_float_dtype(dtype):
                    col_type = "FLOAT"
                
                # Check for datetime
                elif pd.api.types.is_datetime64_dtype(dtype):
                    col_type = "TIMESTAMP"
                
                # For string/object columns, do more detailed inspection
                else:
                    # Default to TEXT
                    col_type = "TEXT"
                    
                    # Skip empty columns
                    if df[col].isna().all():
                        col_type = "TEXT"
                        columns.append(f'"{col}" {col_type}')
                        continue
                    
                    # Check sample of non-null values (up to 100)
                    sample = df[col].dropna().head(100)
                    
                    # Only continue if we have data
                    if len(sample) > 0:
                        # Check if all values might be dates
                        try:
                            pd.to_datetime(sample)
                            col_type = "DATE"
                        except:
                            # Check if values might be booleans
                            if all(str(v).lower() in ('true', 'false', 'yes', 'no', 't', 'f', 'y', 'n', '1', '0') 
                                   for v in sample):
                                col_type = "BOOLEAN"
                            # Check if values might be JSON
                            elif all((str(v).startswith('{') and str(v).endswith('}')) or 
                                    (str(v).startswith('[') and str(v).endswith(']'))
                                    for v in sample):
                                try:
                                    # Try to parse first value as JSON
                                    json.loads(str(sample.iloc[0]))
                                    col_type = "JSONB"
                                except:
                                    pass
                
                columns.append(f'"{col}" {col_type}')
            
            # Create table
            cursor = self.pg_conn.cursor()
            create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
            cursor.execute(create_query)
            
            # Insert data
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
            self.pg_conn.commit()
            cursor.close()
            
            return True, f"Created table '{table_name}' with {len(df)} records"
        except Exception as e:
            return False, f"Error creating PostgreSQL table from CSV: {str(e)}"
    
    def postgres_create_table_from_csv_path(self, table_name, csv_file_path):
        """Create a new PostgreSQL table from a CSV file at the given path"""
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            # Check if file exists
            if not os.path.exists(csv_file_path):
                return False, f"CSV file not found at path: {csv_file_path}"
                
            # Read the CSV file
            df = pd.read_csv(csv_file_path)
            
            # For consistency, use the file-based implementation
            with open(csv_file_path, 'rb') as file:
                return self.postgres_create_table_from_csv(table_name, file)
                
        except Exception as e:
            return False, f"Error creating PostgreSQL table from CSV: {str(e)}"
    
    # MongoDB Operations
    def mongo_list_collections(self):
        """List all collections in MongoDB database"""
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            collections = self.mongo_db.list_collection_names()
            return True, collections
        except Exception as e:
            return False, f"Error listing MongoDB collections: {str(e)}"
    
    def mongo_view_collection(self, collection_name, limit=100):
        """View contents of a specific MongoDB collection"""
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            collection = self.mongo_db[collection_name]
            documents = list(collection.find({}).limit(limit))
            
            # Convert MongoDB documents to pandas DataFrame
            if documents:
                df = pd.DataFrame(documents)
                # Convert ObjectId to string for better display
                if '_id' in df.columns:
                    df['_id'] = df['_id'].astype(str)
            else:
                df = pd.DataFrame()
                
            return True, df
        except Exception as e:
            return False, f"Error viewing MongoDB collection: {str(e)}"
    
    def mongo_count_documents(self, collection_name, filter_query=None):
        """Count documents in a MongoDB collection"""
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            collection = self.mongo_db[collection_name]
            if filter_query is None:
                filter_query = {}
            count = collection.count_documents(filter_query)
            return True, count
        except Exception as e:
            return False, f"Error counting documents in MongoDB collection: {str(e)}"
    
    def mongo_add_document(self, collection_name, document_data):
        """Add a document to a MongoDB collection"""
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            collection = self.mongo_db[collection_name]
            result = collection.insert_one(document_data)
            return True, f"Document added with ID: {result.inserted_id}"
        except Exception as e:
            return False, f"Error adding document to MongoDB collection: {str(e)}"
    
    def mongo_delete_document(self, collection_name, filter_query):
        """Delete documents from a MongoDB collection based on a filter"""
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            collection = self.mongo_db[collection_name]
            result = collection.delete_many(filter_query)
            return True, f"{result.deleted_count} documents deleted"
        except Exception as e:
            return False, f"Error deleting documents from MongoDB collection: {str(e)}"
    
    def mongo_create_collection_from_csv(self, collection_name, csv_file):
        """Create a new MongoDB collection from a CSV file"""
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            # Read the CSV file with improved type detection
            df = pd.read_csv(csv_file)
            
            # Process data types before conversion to dictionaries
            df = self._process_dataframe_types(df)
            
            # Convert DataFrame to list of dictionaries (documents)
            documents = df.to_dict('records')
            
            # Create collection and insert documents
            collection = self.mongo_db[collection_name]
            if documents:
                result = collection.insert_many(documents)
                return True, f"Created collection '{collection_name}' with {len(result.inserted_ids)} documents"
            else:
                return True, f"Created empty collection '{collection_name}'"
        except Exception as e:
            return False, f"Error creating MongoDB collection from CSV: {str(e)}"
    
    def mongo_create_collection_from_csv_path(self, collection_name, csv_file_path):
        """Create a new MongoDB collection from a CSV file at the given path"""
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            # Check if file exists
            if not os.path.exists(csv_file_path):
                return False, f"CSV file not found at path: {csv_file_path}"
                
            # For consistency, use the file-based implementation
            with open(csv_file_path, 'rb') as file:
                return self.mongo_create_collection_from_csv(collection_name, file)
                
        except Exception as e:
            return False, f"Error creating MongoDB collection from CSV: {str(e)}"
    
    def _process_dataframe_types(self, df):
        """Process DataFrame to convert columns to appropriate types
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            DataFrame: Processed DataFrame with appropriate types
        """
        for col in df.columns:
            # If column is all NaN, skip it
            if df[col].isna().all():
                continue
                
            # Get a sample of non-null values
            sample = df[col].dropna().head(100)
            if len(sample) == 0:
                continue
                
            # Try to convert strings to appropriate types
            if df[col].dtype == 'object':
                # Try datetime conversion
                try:
                    df[col] = pd.to_datetime(df[col])
                    continue
                except:
                    pass
                    
                # Check if values might be boolean
                if all(str(v).lower() in ('true', 'false', 'yes', 'no', 't', 'f', 'y', 'n', '1', '0') 
                       for v in sample):
                    # Convert to boolean
                    df[col] = df[col].map(lambda x: str(x).lower() in ('true', 'yes', 't', 'y', '1') 
                                         if pd.notnull(x) else None)
                    continue
                    
                # Check if values might be numeric
                try:
                    # Try integer conversion
                    df[col] = pd.to_numeric(df[col], downcast='integer')
                    continue
                except:
                    try:
                        # Try float conversion
                        df[col] = pd.to_numeric(df[col], downcast='float')
                        continue
                    except:
                        pass
                        
                # Check if values might be JSON
                if all((str(v).startswith('{') and str(v).endswith('}')) or 
                       (str(v).startswith('[') and str(v).endswith(']'))
                       for v in sample):
                    # Try to parse as JSON
                    try:
                        df[col] = df[col].apply(lambda x: json.loads(str(x)) if pd.notnull(x) else None)
                    except:
                        pass
        
        return df
    
    # New PostgreSQL operations
    def postgres_create_table(self, table_name, columns):
        """Create a new PostgreSQL table with specified columns
        
        Args:
            table_name (str): Name of the table to create
            columns (dict): Dictionary mapping column names to their data types
                           (or empty strings for automatic type inference)
        """
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            
            # Format column definitions
            column_defs = []
            valid_types = [
                "TEXT", "VARCHAR", "CHAR", "INTEGER", "INT", "BIGINT", "SMALLINT", 
                "FLOAT", "REAL", "DOUBLE PRECISION", "NUMERIC", "DECIMAL",
                "BOOLEAN", "DATE", "TIMESTAMP", "TIME", "JSON", "JSONB"
            ]
            
            for col_name, col_type in columns.items():
                # Check if a valid type was provided, otherwise infer it
                if not col_type or col_type.upper() not in valid_types:
                    inferred_type = self._infer_type_from_name(col_name)
                    column_defs.append(f'"{col_name}" {inferred_type}')
                else:
                    column_defs.append(f'"{col_name}" {col_type}')
            
            # Create the table
            create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
            cursor.execute(create_query)
            self.pg_conn.commit()
            cursor.close()
            
            return True, f"Table '{table_name}' created successfully"
        except Exception as e:
            return False, f"Error creating PostgreSQL table: {str(e)}"
    
    def postgres_add_column(self, table_name, column_name, column_type):
        """Add a column to an existing PostgreSQL table
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column to add
            column_type (str): SQL data type of the column or will be inferred if not a valid type
        """
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            
            # Check if a valid PostgreSQL type was provided
            valid_types = [
                "TEXT", "VARCHAR", "CHAR", "INTEGER", "INT", "BIGINT", "SMALLINT", 
                "FLOAT", "REAL", "DOUBLE PRECISION", "NUMERIC", "DECIMAL",
                "BOOLEAN", "DATE", "TIMESTAMP", "TIME", "JSON", "JSONB"
            ]
            
            # If not a known type, attempt to infer the type from the column name
            if column_type.upper() not in valid_types:
                inferred_type = self._infer_type_from_name(column_name)
                query = f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {inferred_type}'
            else:
                query = f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {column_type}'
                
            cursor.execute(query)
            self.pg_conn.commit()
            cursor.close()
            
            return True, f"Column '{column_name}' added to table '{table_name}'"
        except Exception as e:
            return False, f"Error adding column to PostgreSQL table: {str(e)}"
    
    def _infer_type_from_name(self, column_name):
        """Infer the most likely data type based on the column name
        
        Args:
            column_name (str): Name of the column
            
        Returns:
            str: Likely PostgreSQL data type for the column
        """
        name_lower = column_name.lower().strip()
        
        # Common ID patterns that should be INTEGER
        id_patterns = ['id', '_id', 'code', 'num', 'number', 'count']
        if name_lower == 'id' or name_lower.endswith('_id') or name_lower.endswith('id') or any(p == name_lower for p in id_patterns):
            return "INTEGER"
            
        # INTEGER types - number-related columns
        if any(pattern in name_lower for pattern in [
            'age', 'year', 'month', 'day', 'quantity', 'qty', 'count', 'num', 
            'number', 'size', 'order', 'points', 'score', 'visits', 'views', 'clicks'
        ]):
            return "INTEGER"
            
        # FLOAT types - numeric with decimal values
        if any(pattern in name_lower for pattern in [
            'price', 'cost', 'fee', 'amount', 'sum', 'total', 'balance', 'rate', 
            'percentage', 'percent', 'discount', 'salary', 'wage', 'height', 'weight', 
            'latitude', 'longitude', 'rating', 'avg', 'average'
        ]):
            return "FLOAT"
            
        # BOOLEAN types - flags and statuses
        if any(pattern in name_lower for pattern in [
            'is_', 'has_', 'can_', 'allow', 'active', 'enabled', 'flag', 'status', 
            'verified', 'approved', 'accepted', 'valid', 'complete', 'done', 'confirmed',
            'remember', 'subscribe', 'notify', 'public', 'visible', 'published'
        ]):
            return "BOOLEAN"
        
        # TEXT types for common text fields
        if any(pattern in name_lower for pattern in [
            'name', 'title', 'description', 'comment', 'message', 'text', 'content', 
            'info', 'details', 'summary', 'address', 'email', 'phone', 'password',
            'hash', 'token', 'key', 'code', 'url', 'link', 'path', 'username', 
            'first_name', 'last_name', 'middle_name', 'job_title', 'occupation',
            'company', 'organization', 'department', 'notes', 'remarks'
        ]):
            return "TEXT"
            
        # DATE types - date-related fields
        if any(pattern in name_lower for pattern in [
            'date', 'dob', 'doj', 'birthday', 'birth', 'joined', 'start_date', 'end_date',
            'hire_date', 'termination_date', 'registration_date', 'expiration_date', 'expiry'
        ]):
            return "DATE"
            
        # TIMESTAMP types - datetime fields
        if any(pattern in name_lower for pattern in [
            'timestamp', 'datetime', 'created_at', 'updated_at', 'modified_at', 'deleted_at',
            'login_time', 'logout_time', 'last_seen', 'last_login', 'last_modified', 
            'time', 'created', 'updated', 'modified', 'last_update'
        ]):
            return "TIMESTAMP"
            
        # JSON/JSONB types - complex data structures
        if any(pattern in name_lower for pattern in [
            'json', 'metadata', 'meta', 'properties', 'attributes', 'config', 'configuration',
            'settings', 'options', 'preferences', 'data', 'params', 'parameters'
        ]):
            return "JSONB"
            
        # Default to TEXT for anything we can't confidently categorize
        return "TEXT"
    
    def postgres_add_multiple_columns(self, table_name, columns_data):
        """Add multiple columns to an existing PostgreSQL table at once
        
        Args:
            table_name (str): Name of the table
            columns_data (dict): Dictionary mapping column names to their data types
                                (or values to infer types from)
        """
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            
            # Format column definitions with type inference
            column_defs = []
            for col_name, col_type_or_value in columns_data.items():
                # If data type is specified directly, use it
                if isinstance(col_type_or_value, str) and col_type_or_value.upper() in [
                    "TEXT", "VARCHAR", "INTEGER", "INT", "BIGINT", "SMALLINT", 
                    "FLOAT", "REAL", "DOUBLE PRECISION", "NUMERIC", "DECIMAL",
                    "BOOLEAN", "DATE", "TIMESTAMP", "TIME", "JSON", "JSONB"
                ]:
                    col_type = col_type_or_value
                # Otherwise, infer type from the value or use TEXT as default
                else:
                    col_type = self._infer_column_type(col_type_or_value)
                
                column_defs.append(f'ADD COLUMN "{col_name}" {col_type}')
            
            # Create the ALTER TABLE statement with multiple ADD COLUMN clauses
            alter_query = f'ALTER TABLE {table_name} {", ".join(column_defs)}'
            cursor.execute(alter_query)
            self.pg_conn.commit()
            cursor.close()
            
            column_names = list(columns_data.keys())
            return True, f"Added columns {', '.join(column_names)} to table '{table_name}'"
        except Exception as e:
            return False, f"Error adding columns to PostgreSQL table: {str(e)}"
    
    def _infer_column_type(self, value):
        """Infer PostgreSQL column type from a sample value
        
        Args:
            value: Sample value to infer type from
            
        Returns:
            str: PostgreSQL data type
        """
        # For None or empty string, default to TEXT
        if value is None or (isinstance(value, str) and not value.strip()):
            return "TEXT"
            
        # If it's a string, check if it matches special formats
        if isinstance(value, str):
            # Try to convert to different types
            value_lower = value.lower().strip()
            
            # Check for null-like strings
            if value_lower in ["null", "none", "nil", "na", "n/a"]:
                return "TEXT"
                
            # Check for boolean values
            if value_lower in ["true", "false", "yes", "no", "t", "f", "y", "n", "1", "0"]:
                return "BOOLEAN"
                
            # Check for common date formats
            import re
            # ISO date format (YYYY-MM-DD)
            iso_date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
            # US date format (MM/DD/YYYY)
            us_date_pattern = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$')
            # European date format (DD/MM/YYYY)
            eu_date_pattern = re.compile(r'^\d{1,2}\.\d{1,2}\.\d{4}$')
            # Other common formats
            other_date_pattern = re.compile(r'^\d{4}\.\d{2}\.\d{2}$|^\d{2}-\d{2}-\d{4}$')
            
            if (iso_date_pattern.match(value) or us_date_pattern.match(value) or 
                eu_date_pattern.match(value) or other_date_pattern.match(value)):
                return "DATE"
                
            # Check for timestamp formats
            timestamp_pattern = re.compile(
                r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}|'  # ISO format
                r'^\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}|'          # US format with time
                r'^\d{2}\.\d{2}\.\d{4}\s\d{2}:\d{2}'         # EU format with time
            )
            if timestamp_pattern.match(value):
                return "TIMESTAMP"
                
            # Check if it can be converted to a number
            # Integer check
            if re.match(r'^[-+]?\d+$', value):
                return "INTEGER"
            # Float check (handles scientific notation too)
            if re.match(r'^[-+]?\d*\.?\d+([eE][-+]?\d+)?$', value):
                    return "FLOAT"
                    
            # Check for JSON-like structure
            if (value.startswith('{') and value.endswith('}')) or (value.startswith('[') and value.endswith(']')):
                try:
                    json.loads(value)
                    return "JSONB"
                except:
                    pass
            
            # Check if it looks like a URL
            url_pattern = re.compile(r'^(http|https|ftp)://[^\s/$.?#].[^\s]*$')
            if url_pattern.match(value):
                return "TEXT"
                
            # Check if it looks like an email
            email_pattern = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
            if email_pattern.match(value):
                return "TEXT"
            
            # Default for strings
            return "TEXT"
            
        # For numeric types
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "FLOAT"
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, (dict, list)):
            return "JSONB"
        
        # Handle date objects
        try:
            import datetime
            if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                return "DATE"
            if isinstance(value, datetime.datetime):
                return "TIMESTAMP"
        except ImportError:
            pass
            
        # Default fallback
        return "TEXT"
    
    def postgres_delete_column(self, table_name, column_name):
        """Delete a column from a PostgreSQL table
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column to delete
        """
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            query = f'ALTER TABLE {table_name} DROP COLUMN "{column_name}"'
            cursor.execute(query)
            self.pg_conn.commit()
            cursor.close()
            
            return True, f"Column '{column_name}' deleted from table '{table_name}'"
        except Exception as e:
            return False, f"Error deleting column from PostgreSQL table: {str(e)}"
    
    def postgres_rename_column(self, table_name, old_column_name, new_column_name):
        """Rename a column in a PostgreSQL table
        
        Args:
            table_name (str): Name of the table
            old_column_name (str): Current name of the column
            new_column_name (str): New name for the column
        """
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            query = f'ALTER TABLE {table_name} RENAME COLUMN "{old_column_name}" TO "{new_column_name}"'
            cursor.execute(query)
            self.pg_conn.commit()
            cursor.close()
            
            return True, f"Column in table '{table_name}' renamed from '{old_column_name}' to '{new_column_name}'"
        except Exception as e:
            return False, f"Error renaming column in PostgreSQL table: {str(e)}"
    
    def postgres_rename_table(self, old_table_name, new_table_name):
        """Rename a PostgreSQL table
        
        Args:
            old_table_name (str): Current name of the table
            new_table_name (str): New name for the table
        """
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            query = f'ALTER TABLE {old_table_name} RENAME TO {new_table_name}'
            cursor.execute(query)
            self.pg_conn.commit()
            cursor.close()
            
            return True, f"Table renamed from '{old_table_name}' to '{new_table_name}'"
        except Exception as e:
            return False, f"Error renaming PostgreSQL table: {str(e)}"
    
    def postgres_update_row(self, table_name, set_values, condition):
        """Update rows in a PostgreSQL table
        
        Args:
            table_name (str): Name of the table
            set_values (dict): Dictionary of column-value pairs to update
            condition (str): WHERE condition to identify rows to update
        """
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            cursor = self.pg_conn.cursor()
            
            # Format SET clause
            set_clause = ", ".join([f'"{col}" = %s' for col in set_values.keys()])
            values = list(set_values.values())
            
            query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
            cursor.execute(query, values)
            updated_count = cursor.rowcount
            self.pg_conn.commit()
            cursor.close()
            
            return True, f"{updated_count} rows updated in table '{table_name}'"
        except Exception as e:
            return False, f"Error updating rows in PostgreSQL table: {str(e)}"
    
    def postgres_run_query(self, query, params=None):
        """Run a custom SQL query on PostgreSQL
        
        Args:
            query (str): SQL query to execute
            params (list, optional): Parameters for the query
        """
        if not self.pg_conn:
            status, message = self.connect_postgres()
            if not status:
                return False, message
        
        try:
            # Detect if it's a SELECT query
            is_select = query.strip().upper().startswith("SELECT")
            
            if is_select:
                # For SELECT queries, return results as a DataFrame
                if params:
                    df = pd.read_sql_query(query, self.pg_conn, params=params)
                else:
                    df = pd.read_sql_query(query, self.pg_conn)
                return True, df
            else:
                # For non-SELECT queries, execute and return affected rows
                cursor = self.pg_conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                affected_rows = cursor.rowcount
                self.pg_conn.commit()
                cursor.close()
                
                return True, f"Query executed successfully. Affected rows: {affected_rows}"
        except Exception as e:
            return False, f"Error executing SQL query: {str(e)}"
    
    # New MongoDB operations
    def mongo_create_collection(self, collection_name):
        """Create a new MongoDB collection
        
        Args:
            collection_name (str): Name of the collection to create
        """
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            # In MongoDB, collections are created implicitly when first document is inserted
            # But we can explicitly create it this way
            self.mongo_db.create_collection(collection_name)
            return True, f"Collection '{collection_name}' created successfully"
        except Exception as e:
            return False, f"Error creating MongoDB collection: {str(e)}"
    
    def mongo_rename_collection(self, old_collection_name, new_collection_name):
        """Rename a MongoDB collection
        
        Args:
            old_collection_name (str): Current name of the collection
            new_collection_name (str): New name for the collection
        """
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            collection = self.mongo_db[old_collection_name]
            collection.rename(new_collection_name)
            return True, f"Collection renamed from '{old_collection_name}' to '{new_collection_name}'"
        except Exception as e:
            return False, f"Error renaming MongoDB collection: {str(e)}"
    
    def mongo_update_document(self, collection_name, filter_query, update_data):
        """Update documents in a MongoDB collection
        
        Args:
            collection_name (str): Name of the collection
            filter_query (dict): Query to match documents to update
            update_data (dict): Update operations to apply
        """
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            collection = self.mongo_db[collection_name]
            
            # If update_data doesn't use MongoDB operators ($set, etc.), wrap it in $set
            if not any(key.startswith('$') for key in update_data.keys()):
                update_data = {'$set': update_data}
            
            result = collection.update_many(filter_query, update_data)
            return True, f"{result.modified_count} documents updated in collection '{collection_name}'"
        except Exception as e:
            return False, f"Error updating documents in MongoDB collection: {str(e)}"
    
    def mongo_run_aggregation(self, collection_name, pipeline):
        """Run an aggregation pipeline on a MongoDB collection
        
        Args:
            collection_name (str): Name of the collection
            pipeline (list): List of aggregation pipeline stages
        """
        if self.mongo_db is None:
            status, message = self.connect_mongo()
            if not status:
                return False, message
        
        try:
            collection = self.mongo_db[collection_name]
            
            # Run the aggregation pipeline
            result = list(collection.aggregate(pipeline))
            
            # Convert to DataFrame if possible
            if result:
                # Convert ObjectId to string for better display
                result_str = json.loads(json_util.dumps(result))
                df = pd.DataFrame(result_str)
                return True, df
            else:
                return True, "Aggregation returned no results"
        except Exception as e:
            return False, f"Error running MongoDB aggregation: {str(e)}" 