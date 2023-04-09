from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

from faker import Faker

from data_generation_pyspark import generate_testing_data

fake = Faker()


# schema = StructType([
#     StructField('id', IntegerType(), nullable=False),
#     StructField('name', StringType(), nullable=False),
#     StructField('age', IntegerType(), nullable=False),
#     StructField('gender', StringType(), nullable=False)
# ])

#Schema based on Excel File
schema = StructType([
    StructField('order', IntegerType(), nullable=False),
    StructField('ordercreatedt', DateType(), nullable=False),
    StructField('Currentstatus', StringType(), nullable=True),
    StructField('Componentstatus', StringType(), nullable=True)
])

# Define the enumerations using constant data
# enumerations = {
#     'name': ['John', 'Jane', 'Bob', 'Alice'],
#     'gender': ['male', 'female', 'other']
# }


# Define the enumerations using faker package
# enumerations = {
#     'name': [fake.name() for _ in range(100)],
#     'gender': [fake.random_element(elements=('male', 'female', 'other')) for _ in range(100)]
# }

# Define the enumerations based on Excel schema
enumerations = {
    'Currentstatus': ['Active', 'Partial Cancel', 'Cancelled Same-day', 'Cancelled'],
    'Componentstatus': ['Active', 'Refunded', 'Exchanged', 'Cancelled']
}


# Generate testing data in CSV format with the provided schema and enumerations
csv_data = generate_testing_data(format="CSV", schema=schema, enumerations=enumerations, dataset_size=10)
# Print the generated CSV data
print(csv_data)

# Generate testing data in JSON format with the provided schema and enumerations
# json_data = generate_testing_data(format="JSON", schema=schema, enumerations=enumerations, dataset_size=10)
# Print the generated JSON data
# print(json_data)

# Generate testing data in AVRO format with the provided schema and enumerations
# avro_data = generate_testing_data(format="AVRO", schema=schema, enumerations=enumerations, dataset_size=10)
# Print the generated AVRO data
# print(avro_data)