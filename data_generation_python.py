import random
import string
import csv
import json
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

def generate_testing_data(format, schema=None, enumerations=None, dataset_size=100):
    """
    Generate testing data in the given format using the given schema and enumerations.
    Returns the generated data as a string.
    """
    if format not in ['CSV', 'JSON', 'AVRO']:
        raise ValueError("Invalid format specified. Valid options are 'CSV', 'JSON', or 'AVRO'.")

    data = []
    if schema:
        fields = schema['fields']
        field_names = [field['name'] for field in fields]

        for i in range(dataset_size):
            row = {}
            for field in fields:
                field_name = field['name']
                field_type = field['type']

                if field_type == 'string':
                    if field_name in enumerations:
                        row[field_name] = random.choice(enumerations[field_name])
                    else:
                        row[field_name] = ''.join(random.choices(string.ascii_letters + string.digits, k=10))

                elif field_type == 'int':
                    row[field_name] = random.randint(0, 100)

                # Add more field types as needed

            data.append(row)

    if format == 'CSV':
        csv_str = ','.join(field_names) + '\n'
        for row in data:
            csv_str += ','.join(str(row[field]) for field in field_names) + '\n'
        return csv_str

    elif format == 'JSON':
        return json.dumps(data)

    elif format == 'AVRO':
        avro_schema = {
            "type": "record",
            "name": "test_data",
            "fields": schema['fields']
        }

        with open('test_data.avro', 'wb') as avro_file:
            writer = DataFileWriter(avro_file, DatumWriter(), avro_schema)
            for row in data:
                writer.append(row)
            writer.close()

        with open('test_data.avro', 'rb') as avro_file:
            return avro_file.read()


# Example usage
schema = {
    "type": "record",
    "name": "test_data",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "gender", "type": "string", "default": "unknown"}
    ]
}

enumerations = {
    "gender": ["male", "female", "other"]
}

csv_data = generate_testing_data(format="CSV", schema=schema, enumerations=enumerations, dataset_size=10)
json_data = generate_testing_data(format="JSON", schema=schema, enumerations=enumerations, dataset_size=10)
avro_data = generate_testing_data(format="AVRO", schema=schema, enumerations=enumerations, dataset_size=10)

print("CSV data:")
print(csv_data)

print("JSON data:")
print(json_data)

print("AVRO data:")
print(avro_data)
