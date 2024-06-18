import pyodbc

# Define the connection parameters
server = 'your_server_name_or_ip'
database = 'your_database_name'
username = 'your_username'
password = 'your_password'

# Create the connection string
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Establish the connection
try:
    connection = pyodbc.connect(connection_string)
    print("Connection established successfully.")
except Exception as e:
    print(f"Error: {e}")

# Create a cursor object using the connection
cursor = connection.cursor()

# Example query
query = 'SELECT * FROM your_table_name'

# Execute the query
try:
    cursor.execute(query)
    # Fetch and print all rows
    rows = cursor.fetchall()
    for row in rows:
        print(row)
except Exception as e:
    print(f"Error: {e}")

# Close the connection
connection.close()
