import pandas as pd
import json
import random
from datetime import datetime, timedelta
from faker import Faker


def generate_testing_data(schema, enumerations, dataset_size, output_filename=""):
    fake = Faker()
    primary_key_dict = config["primary_key_columns"]
    data = []

    for _ in range(dataset_size):
        row = {}

        for field in schema:
            field_name = field['name']
            field_type = field['type']

            if field_type == "IntegerType":
                if field_name in primary_key_dict:
                    primary_key_col = random.randint(1, dataset_size) + primary_key_dict[field_name]
                    row[field_name] = primary_key_col
                else:
                    row[field_name] = random.randint(1, 100000)

            elif field_type == "StringType":
                if field_name in enumerations:
                    enum_values = enumerations[field_name]
                    row[field_name] = random.choice(enum_values)
                else:
                    row[field_name] = fake.word()

            elif field_type == "FloatType":
                row[field_name] = round(random.uniform(1, 10000), 4)

            elif field_type == "DateType":
                row[field_name] = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')

            elif field_type == "TimestampType":
                start_timestamp = datetime.strptime("2023-01-01T00:00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")
                end_timestamp = datetime.strptime("2023-12-31T23:59:59.999+0000", "%Y-%m-%dT%H:%M:%S.%f%z")
                num_seconds = int((end_timestamp - start_timestamp).total_seconds())
                random_timestamp = start_timestamp + timedelta(seconds=random.randint(0, num_seconds))
                row[field_name] = random_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f%z')

        data.append(row)

    df = pd.DataFrame(data)

    if output_filename:
        csv_filepath = "{}_csv.csv".format(output_filename)
        df.to_csv(csv_filepath, index=False, header=True)
        dat_filepath = "{}.dat".format(output_filename)
        df.to_csv(dat_filepath, sep=",", index=False, header=False)

def test_data_generation_method():
    global config
    fake = Faker()
    json_path = 'test_data_schema.json'
    with open(json_path) as f:
        config = json.load(f)
    dataset_size = config['dataset_size']

    for i in range(1, config['no_of_schema'] + 1):
        schema = config['schema' + '_' + str(i)]['fields']
        enumerations = config['enumerations' + '_' + str(i)]
        schema_name = config["schema_" + str(i)]['schema_name']

        generate_testing_data(schema, enumerations, dataset_size, output_filename=schema_name)

test_data_generation_method()
