from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

packages = "org.apache.spark:spark-avro_2.12:3.3.2"
spark = SparkSession.builder.appName("DataGeneration").config("spark.jars.packages", packages).getOrCreate()
# spark = SparkSession.builder.appName("DataGenerator").getOrCreate()

def generate_testing_data(format: str, schema: StructType, enumerations: dict, dataset_size: int) -> str:
    """
    Generates testing data based on the provided inputs.
    :param format: The format in which the data needs to be produced (CSV, JSON, or AVRO).
    :param schema: The schema for the output data (for CSV or JSON outputs).
    :param enumerations: A dictionary containing column names and their possible values (enumerations).
    :param dataset_size: The number of rows to generate in the output data.
    :return: A string containing the generated data in the specified format.
    """

    # df = spark.range(dataset_size).withColumn("name", randn()).withColumn("age", (rand() * 100 + 1).cast(
    #     IntegerType())).withColumn("gender", when(rand() < 0.33, "Male").when(rand() < 0.66, "Female").otherwise("Other"))
    # Create a DataFrame with random data
    df = spark.range(0, dataset_size).withColumn('rand', rand(seed=42)).withColumn('randn', randn(seed=42))


    # Apply enumerations to the DataFrame
    for field in schema.fields:
        if field.dataType == IntegerType():
            df = df.withColumn(field.name, (rand() * 100 + 1).cast(IntegerType()))
        elif field.dataType == StringType():
            if field.name in enumerations:
                enum_values = enumerations[field.name]
                enum_expr = when(rand() < 1.0 / len(enum_values), enum_values[0])
                for enum_value in enum_values[1:]:
                    enum_expr = enum_expr.when(rand() < 1.0 / len(enum_values), enum_value)
                df = df.withColumn(field.name, enum_expr.otherwise(enum_values[0]))
            else:
                df = df.withColumn(field.name, randn(10))

    # Drop rand and randn columns
    df = df.drop("rand", "randn")

    # Write the DataFrame to the specified format and return the generated data
    # Write dataframe to CSV file
    if format == "CSV":
        # Write the generated data to a CSV file
        #df.write.format("csv").mode('overwrite').option("header", "true").save("output.csv")
        df.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").option("quoteAll", "true").save(
            "output.csv")
        print("CSV testing data generated successfully.")
        csv_data = df.toPandas().to_csv(index=False)
        return csv_data

    # Write dataframe to JSON file
    elif format == "JSON":
        #df.toJSON().saveAsTextFile("output.json")
        df.coalesce(1).write.format("json").mode('overwrite').save("output.json")
        print("JSON testing data generated successfully.")
        json_data = df.toJSON().collect()
        return json_data

    # Write dataframe to JSON file
    elif format == "AVRO":
        # convert Spark DataFrame to Avro format
        #avro_data = df.write.format("avro").save("output.avro")
        avro_data = df.coalesce(1).write.format("avro").mode('overwrite').save("output.avro")
        print("AVRO testing data generated successfully.")
        return avro_data

    else:
        print("Invalid format provided. Please provide either 'CSV', 'JSON', or 'AVRO'.")
        return None


schema = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('name', StringType(), nullable=False),
    StructField('age', IntegerType(), nullable=False),
    StructField('gender', StringType(), nullable=False)
])

enumerations = {
    'name': ['John', 'Jane', 'Bob', 'Alice'],
    'gender': ['male', 'female', 'other']
}

# Generate testing data in CSV format with the provided schema and enumerations
csv_data = generate_testing_data(format="CSV", schema=schema, enumerations=enumerations, dataset_size=10)
# Print the generated CSV data
print(csv_data)

# Generate testing data in JSON format with the provided schema and enumerations
json_data = generate_testing_data(format="JSON", schema=schema, enumerations=enumerations, dataset_size=10)
# Print the generated JSON data
print(json_data)

# Generate testing data in AVRO format with the provided schema and enumerations
avro_data = generate_testing_data(format="AVRO", schema=schema, enumerations=enumerations, dataset_size=10)
# Print the generated AVRO data
print(avro_data)