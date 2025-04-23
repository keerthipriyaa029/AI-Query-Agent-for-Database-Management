# AI Query Agent for Database Management: An Intelligent Database Agent for PostgreSQL & MongoDB

A powerful chatbot with natural language using streamlit interface that serves as an intelligent agent between users and databases, allowing interaction with PostgreSQL and MongoDB through conversational queries powered by Large Language Models.

## Features

- **Natural Language Interface:** Communicate with your databases using plain English queries
- **Multi-Database Support:** Works with both PostgreSQL and MongoDB databases seamlessly
- **Complete CRUD Operations:**
  - Create tables and collections directly through chat or CSV upload
  - Read data with filters and limits
  - Update records and documents with custom conditions
  - Delete records with flexible criteria
- **Schema Management:**
  - Add, delete, and rename columns
  - Rename tables and collections
  - Change data types with intelligent inference
- **Advanced Querying:**
  - Run custom SQL queries with joins and subqueries
  - Perform MongoDB aggregations
  - Execute arithmetic and logical operations
- **Data Upload:** Create new tables/collections by uploading CSV files
- **Interactive UI:** User-friendly Streamlit interface with chat history
- **Intelligent Type Inference:** Automatically suggests appropriate data types for columns based on names and content
- **LLM Integration:** Leverages OpenAI's language models to understand and process natural language queries

## Project Structure

```
db_chatbot/
├── app.py                     # Main Streamlit app with UI & chatbot logic
├── db_utils.py                # Handles PostgreSQL and MongoDB functions (CRUD, upload, etc.)
├── agent.py                   # LLM-based agent logic for parsing and executing user queries
├── config.py                  # DB connection configs (Postgres & Mongo) + API keys
└── requirements.txt           # All dependencies
```

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd db_chatbot
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your database credentials and OpenAI API key:
```
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=test

# OpenAI API Key for LLM agent
OPENAI_API_KEY=your_openai_api_key_here
LLM_MODEL=gpt-3.5-turbo
```

## Usage

1. Start the Streamlit application:
```bash
streamlit run app.py
```

2. Access the application in your web browser at `http://localhost:8501`

3. Configure your database connections in the sidebar

4. Start chatting with the assistant using natural language queries:
   - "Show me all tables in Postgres"
   - "How many documents are in the users collection?"
   - "Add a new customer with name John Doe and email john@example.com"
   - "Delete records from products where price > 100"

5. To upload a CSV file and create a new table/collection:
   - Use the file uploader at the bottom of the interface
   - Select your target database (PostgreSQL or MongoDB)
   - Provide a name for the new table/collection
   - Click "Create from CSV"

## Example Queries

### Basic Operations
- **Listing:** "List all collections in MongoDB", "Show me all postgres tables"
- **Viewing:** "Show me the first 10 rows in customers table", "Let me see all products in the MongoDB collection"
- **Counting:** "How many users do we have?", "Count records in the orders table"

### Create and Modify
- **Creating Tables:** "Create a new table called employees with columns name (text), age (int), and salary (float)"
- **Creating Collections:** "Create a MongoDB table named projects"
- **Adding Data:** "Add a new product with name 'Phone' and price 599.99 to the products table"
- **Updating Data:** "Update the price to 499.99 for the product where id = 5"
- **Deleting Records:** "Delete all users where last_login is before 2022", "Remove document with id 12345 from mongo"

### Schema Modifications
- **Adding Columns:** "Add a status column of type text to the orders table"
- **Removing Columns:** "Delete the age column from the users table"
- **Renaming Columns:** "Rename the 'user_name' column to 'username' in customers table"
- **Changing Table Names:** "Rename the products table to items"

### Advanced Queries
- **SQL Joins:** "Show me customers and their orders with a join"
- **Arithmetic Operations:** "Calculate the average price of products"
- **Aggregations:** "Group documents by category and count them"
- **Custom SQL:** "Run SQL query: SELECT products.name, categories.name FROM products JOIN categories ON products.category_id = categories.id"

## Requirements

- Python 3.12.8 (fully compatible with all dependencies)
- PostgreSQL database
- MongoDB database
- OpenAI API key (for the LLM agent)

## Technologies Used

- **Streamlit**: For building the interactive web interface
- **LangChain**: For creating and managing the LLM-based agent
- **OpenAI API**: Powers the natural language understanding capability
- **psycopg2**: For PostgreSQL database connectivity
- **pymongo**: For MongoDB database connectivity
- **pandas**: For data manipulation and display
- **python-dotenv**: For environment variable management

