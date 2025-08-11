from influxdb_client import InfluxDBClient

token = "-pi9FAAHD7gjxwQ5DfT1PVop0K3KBVUvoXZFQlJSMP_WMH2icU645gKKYu6P8I8wpg4Flm6WO4KKziaEr4iKYg=="
org = "Ruhuna_Eng"
bucket = "Sensor"

client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
query = f'from(bucket: "{bucket}") |> range(start: -1h)'

tables = client.query_api().query(query, org=org)
for table in tables:
    for record in table.records:
        print(f"{record.get_time()} - {record.get_field()} = {record.get_value()}")
