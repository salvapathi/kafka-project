import os
dependies_path=r"/opt/airflow/scripts" #folder namee
os.chdir(dependies_path)
exec(open(r"/opt/airflow/scripts/python_libraries.py").read())
exec(open(r"/opt/airflow/scripts/credential.py").read()) 
# exec(open(r"/opt/airflow/scripts/kafka_scripts/super_bikesscripts.py").read()) 


column_regex_mapping = {
    "super_bike": "[^a-zA-Z0-9\s]",
    "manufacturer": "[^a-zA-Z0-9\s]",
    "top_speed": "[^0-9]",
    "engine_displacement_cc":"[^0-9]",
    "horsepower":"[^0-9]",
    "price_dollars":"[^0-9]",
    "maintenance_cost_year":"[^0-9]"
    
}
def data_wrangling(data, column_regex_mapping):
    
    if data.empty:
        print("The input DataFrame is empty.")
        return None
    
    # Convert DataFrame to CSV string
    csv_string = StringIO()
    data.to_csv(csv_string, index=False)
    
    # Move the cursor to the beginning of the StringIO object
    csv_string.seek(0)
    
    pandas_df = pd.read_csv(csv_string)
    
    
    # Convert Pandas DataFrame to Spark DataFrame
    data =spark.createDataFrame(pandas_df)
    data.show()
    for column, regex in column_regex_mapping.items():
        data = data.withColumn(column, regexp_replace(column, regex,""))
    return data
        

# Example usage:
# Apply data wrangling function
def schema(data):
    data = data.withColumn("origin_year", regexp_extract(data["origin_year"], r"\b?(\d{4})\b?", 1))\
        .withColumn("price_dollars",col("price_dollars").cast("double"))\
        .withColumn("maintenance_cost_year",col("maintenance_cost_year").cast("int"))\
        .withColumn("horsepower",col("horsepower").cast("int"))\
        .withColumn("top_speed",col("top_speed").cast("int"))\
        .withColumn("engine_displacement_cc",col("engine_displacement_cc").cast("int"))
    data=data.toPandas()
    data['time_stamp'] = pd.to_datetime(data['time_stamp'])
    return data


def get_last_processed_timestamp():
    # Connect to MySQL database
    connection = mysql.connector.connect(
        host=your_mysql_host,
        user=your_mysql_user,
        password=your_mysql_password,
        database=your_mysql_database
    )

    # Create a cursor
    cursor = connection.cursor()

    try:
        # Execute SQL query to get the timestamp of the most recent record
        cursor.execute("SELECT MAX(time_stamp) FROM bike_data")
        last_processed_timestamp = cursor.fetchone()[0]

        # Return the timestamp
        return last_processed_timestamp

    finally:
        # Close the cursor and database connection
        cursor.close()
        connection.close()





def mysql_connect(data, your_mysql_host, your_mysql_user, your_mysql_password, your_mysql_database, last_processed_timestamp):
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=your_mysql_host,
            user=your_mysql_user,
            password=your_mysql_password,
            database=your_mysql_database
        )

        if connection.is_connected():
            print("Connected to the MySQL server")

            # Name of the table in the MySQL database
            mysql_table = 'bike_data'
            if last_processed_timestamp is None:
                filtered_data=data
                print("Processing all data as fresh batch")
            else:
            # Filter data based on the timestamp column
                # data=data.toPandas()
                filtered_data = data[data['time_stamp'] > last_processed_timestamp]

            if not filtered_data.empty:
                # Create SQLAlchemy engine
                engine = create_engine(f'mysql+mysqlconnector://{your_mysql_user}:{your_mysql_password}@{your_mysql_host}/{your_mysql_database}')

                # Upload DataFrame to MySQL database
                filtered_data.to_sql(name=mysql_table, con=engine, if_exists='append', index=False)

                print("Data uploaded to MySQL successfully")
            else:
                print("No fresh batch data to upload to MySQL")
    except mysql.connector.Error as error:
        print("Error while connecting to MySQL:", error)
    finally:
        # Close the MySQL connection
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed")





#dag code



# Define the tasks

client =server
db = client[data_base_name]
collection = db[collections_name]
cursor = collection.find({})  # You can add query filters if needed
data = list(cursor)

# Flatten nested JSON data
flattened_data = pd.json_normalize(data)

# Convert flattened data to DataFrame
data= pd.DataFrame(flattened_data)


mapping = {
    "_id": "id",
    "bike.name": "super_bike",
    "bike.manufacturer": "manufacturer",
    "bike.top_speed": "top_speed",
    "bike.engine_displacement": "engine_displacement_cc",
    "bike.horsepower": "horsepower",
    "origin.country": "origin_country",
    "origin.year": "origin_year",
    "price_and_maintenance.price": "price_dollars",
    "price_and_maintenance.maintenance_cost": "maintenance_cost_year",
    "timestamp":"time_stamp"
}
data.rename(columns=mapping,inplace=True)

data = data_wrangling(data, column_regex_mapping)
data=schema(data)
last_processed_timestamp = get_last_processed_timestamp()
print("Last processed timestamp:", last_processed_timestamp)
mysql_connect(data, your_mysql_host, your_mysql_user, your_mysql_password, your_mysql_database, last_processed_timestamp)
