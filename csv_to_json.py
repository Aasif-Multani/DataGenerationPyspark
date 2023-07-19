import pandas as pd
import json
import uuid

# Function to convert 'True'/'False' to 'Y'/'N'
def convert_boolean_to_yn(value):
    return 'Y' if value else 'N'

# Read the CSV file and create JSON files
def create_json_files(input_file):
    df = pd.read_csv(input_file)

    # Group rows by 'DatasetName'
    data_by_dataset = df.groupby('DatasetName').apply(lambda x: x.to_dict(orient='records'))

    # Create JSON files for each 'DatasetName'
    for dataset_name, data in data_by_dataset.items():
        guid = str(uuid.uuid4())  # Random GUID
        version_id = "1.0.0"
        source_type = "SEAL"
        source_id = "111178"
        owner_seal_id = "111178"
        dataset_description = "Complete data description"
        contact_email_address = "sandeep.akula@chase.com"
        unique_record_identifier = None

        fields = []
        for i, row in enumerate(data):
            field = {
                "fieldName": row['fieldName'],
                "datatype": row['dataType'].upper(),
                "fieldLength": row['fieldLength'],
                "nullAllowed": convert_boolean_to_yn(row['nullAllowed']),
                "scale": int(row['scale']),
                "precision": int(row['precision']),
                "dateFormat": row['dateFormat'],
                "fieldposition": i + 1,
                "businessMetadata": row['businessMetadata']
            }
            fields.append(field)

        # Create the final JSON structure
        json_data = {
            "guid": guid,
            "versionId": version_id,
            "datasetName": dataset_name,
            "sourceType": source_type,
            "sourceId": source_id,
            "ownerSealId": owner_seal_id,
            "datasetDescription": dataset_description,
            "contactEmailAddress": contact_email_address,
            "uniqueRecordIdentifier": unique_record_identifier,
            "fields": fields
        }

        # Save JSON to a file
        output_file = f"{dataset_name}.json"
        with open(output_file, 'w') as jsonfile:
            json.dump(json_data, jsonfile, indent=2)

# Call the function with your CSV file name
input_csv_file = "your_csv_file.csv"
create_json_files(input_csv_file)
