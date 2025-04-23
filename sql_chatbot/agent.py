# type: ignore
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.chat_models import ChatOpenAI
import json
import re
from config import OPENAI_API_KEY, LLM_MODEL

class QueryAgent:
    def __init__(self):
        """Initialize the query agent with LLM and prompt template"""
        self.llm = ChatOpenAI(api_key=OPENAI_API_KEY, model=LLM_MODEL, temperature=0)
        
        # Define a prompt template that helps the LLM understand database queries
        self.prompt_template = PromptTemplate(
            input_variables=["query"],
            template="""
            You are a database assistant that helps users interact with PostgreSQL and MongoDB databases.
            Your task is to analyze the user's query and determine what database operation they want to perform.
            
            The available operations are:
            1. list_tables - List all tables in PostgreSQL
            2. list_collections - List all collections in MongoDB
            3. view_table - View the contents of a PostgreSQL table
            4. view_collection - View the contents of a MongoDB collection
            5. count_records - Count records in a PostgreSQL table
            6. count_documents - Count documents in a MongoDB collection
            7. add_record - Add a record to a PostgreSQL table
            8. add_document - Add a document to a MongoDB collection
            9. delete_record - Delete records from a PostgreSQL table
            10. delete_document - Delete documents from a MongoDB collection
            11. create_table_from_csv - Create a PostgreSQL table from a CSV file
            12. create_collection_from_csv - Create a MongoDB collection from a CSV file
            13. create_table - Create a new PostgreSQL table with specified columns
            14. create_collection - Create a new MongoDB collection
            15. add_column - Add a column to a PostgreSQL table
            16. add_multiple_columns - Add multiple columns to a PostgreSQL table
            17. delete_column - Delete a column from a PostgreSQL table
            18. update_row - Update values in a row in PostgreSQL
            19. update_document - Update a document in MongoDB
            20. rename_table - Rename a PostgreSQL table
            21. rename_collection - Rename a MongoDB collection
            22. rename_column - Rename a column in a PostgreSQL table
            23. run_query - Run a custom SQL query for PostgreSQL
            24. run_aggregation - Run an aggregation pipeline for MongoDB
            
            IMPORTANT:
            - When the user mentions "table", determine if they're referring to PostgreSQL or MongoDB based on context. 
              If unclear, assume PostgreSQL for "table" and MongoDB for "collection".
            - For add/update operations, extract ALL fields provided by the user, even partial information.
            - If user mentions multiple columns to add (like "add columns name, age, salary to table employees"), use add_multiple_columns operation.
            - If the user's query has a format like "create column names X, Y, Z in table ABC", interpret this as add_multiple_columns.
            - When users add columns through chat, ALWAYS infer appropriate data types for those columns based on their names.
            - For join operations or complex queries, use the run_query operation with the appropriate SQL.
            - For arithmetic and aggregation operations in MongoDB, use run_aggregation.
            
            DATA TYPE HANDLING:
            - When adding columns, try to infer the appropriate data type from context.
            - Common PostgreSQL data types: TEXT, VARCHAR, INTEGER, FLOAT, BOOLEAN, DATE, TIMESTAMP, JSONB
            - If the user specifies a type explicitly (e.g., "add an age column as integer"), use that type.
            - If the user doesn't specify, look for clues in the name or context:
              * Columns like 'id', 'count', 'number', 'age', 'year', 'quantity' typically use INTEGER type
              * Columns like 'price', 'amount', 'rate', 'cost', 'salary', 'fee' typically use FLOAT type
              * Columns like 'is_active', 'has_paid', 'verified', 'active', 'enabled', 'status' typically use BOOLEAN type
              * Columns like 'date', 'dob', 'birthday', 'birth_date', 'created_at', 'start_date' typically use DATE type
              * Columns like 'timestamp', 'created_at', 'updated_at', 'last_login', 'modified_at' typically use TIMESTAMP type
              * Columns like 'name', 'description', 'title', 'email', 'phone', 'address', 'occupation' typically use TEXT type
              * Default to TEXT for column names that don't clearly match any of the above patterns
            - For MongoDB collections, data types are handled automatically but object structure is preserved
            
            EXAMPLE QUERIES AND RESPONSES:
            
            Query: "Add columns name, age, email, and is_active to the users table"
            Response:
            {
                "operation": "add_multiple_columns",
                "target": "users",
                "parameters": {
                    "columns_data": {
                        "name": "TEXT",
                        "age": "INTEGER",
                        "email": "TEXT",
                        "is_active": "BOOLEAN"
                    }
                },
                "explanation": "Adding multiple columns to the users table with appropriate data types"
            }
            
            Query: "Create a new column DOB in customers table"
            Response:
            {
                "operation": "add_column",
                "target": "customers",
                "parameters": {
                    "column_name": "DOB",
                    "column_type": "DATE"
                },
                "explanation": "Adding a DOB (date of birth) column with DATE type to the customers table"
            }
            
            Given this user query: "{query}"
            
            Determine:
            1. The operation to perform (one of the operations listed above)
            2. The target (table name or collection name) if applicable
            3. Any additional parameters needed for the operation
            
            Format your response as a JSON object with these fields:
            {{
                "operation": "operation_name",
                "target": "target_name",
                "parameters": {{ relevant parameters as key-value pairs }},
                "explanation": "Brief explanation of the interpreted query"
            }}
            
            When handling multiple columns, use a format like:
            {{
                "operation": "add_multiple_columns",
                "target": "table_name",
                "parameters": {{
                    "columns_data": {{
                        "name": "TEXT",
                        "age": "INTEGER",
                        "email": "TEXT",
                        "is_active": "BOOLEAN"
                    }}
                }},
                "explanation": "Adding multiple columns to the table with appropriate data types"
            }}
            
            If the user query is unclear or doesn't match any of the available operations, respond with:
            {{
                "operation": "unknown",
                "explanation": "I couldn't understand the database operation from your query. Could you please rephrase?"
            }}
            
            Respond ONLY with the JSON, no other text.
            """
        )
        
        self.chain = LLMChain(llm=self.llm, prompt=self.prompt_template)
    
    def parse_query(self, query):
        """Parse user query using the LLM agent"""
        try:
            # Get response from LLM
            response = self.chain.run(query=query)
            
            # Clean the response to ensure it's valid JSON
            # Sometimes LLM adds backticks or other formatting
            response = re.sub(r'^```json', '', response)
            response = re.sub(r'```$', '', response)
            response = response.strip()
            
            # Parse JSON response
            result = json.loads(response)
            return result
        except Exception as e:
            # Return error response if parsing fails
            return {
                "operation": "error",
                "explanation": f"Error parsing query: {str(e)}"
            }
    
    def is_upload_query(self, query):
        """Determine if a query is about uploading a CSV file"""
        upload_keywords = ["upload", "csv", "file", "import", "create from"]
        return any(keyword in query.lower() for keyword in upload_keywords)
    
    def format_response(self, result):
        """Format the database operation result into a human-readable message"""
        if isinstance(result, tuple) and len(result) == 2:
            status, data = result
            
            if status:
                if isinstance(data, list):
                    if not data:
                        return "No items found."
                    return f"Found {len(data)} items: {', '.join(str(item) for item in data)}"
                elif isinstance(data, int):
                    return f"Count: {data}"
                elif hasattr(data, 'to_html'):  # Check if it's a pandas DataFrame
                    return data  # Return DataFrame directly to display in Streamlit
                else:
                    return str(data)
            else:
                return f"Error: {data}"
        else:
            return "Invalid result format from database operation." 