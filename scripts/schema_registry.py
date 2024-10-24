import requests
import json

schema_registry_url = "http://localhost:8081"
subject = "suppliers"
schema = {
    "type": "record",
    "name": "Supplier",
    "namespace": "com.dev.supplychain",
    "fields": [
        {"name": "SupplierID", "type": "string"},
        {"name": "SupplierName", "type": "string"},
        {"name": "City", "type": "string"},
        {"name": "State", "type": "string"},
        {"name": "ZipCode", "type": "string"},
        {"name": "Country", "type": "string"},
        {"name": "Rating", "type": "int"}
    ]
}

response = requests.post(
    f"{schema_registry_url}/subjects/{subject}/versions",
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    data=json.dumps({"schema": json.dumps(schema)})
)

if response.status_code == 200:
    print("Schema registered successfully:", response.json())
else:
    print("Failed to register schema:", response.text)
