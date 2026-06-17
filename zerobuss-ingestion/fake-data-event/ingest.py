import logging

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from device_event import generate_events

# See "Get your workspace URL and Zerobus Ingest endpoint" for information on obtaining these values.
SERVER_ENDPOINT="7474653312499015.zerobus.us-east-2.cloud.databricks.com"
DATABRICKS_WORKSPACE_URL="https://dbc-e4a968c6-8d14.cloud.databricks.com"
TABLE_NAME="mlpractice.bronze.zerobus_device_data"
CLIENT_ID="12ac1d60-3145-414a-a9fe-0ea7ace984f7"
CLIENT_SECRET="dose024d1ca830eec84a16a86fe035c76ba4"

sdk = ZerobusSdk(
    SERVER_ENDPOINT,
    DATABRICKS_WORKSPACE_URL
)

table_properties = TableProperties(
    TABLE_NAME
)
options = StreamConfigurationOptions(record_type=RecordType.JSON)
stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)

"""try:
    for i in range(100):
        _payload = generate_events(10000+i)

        offset = stream.ingest_record_offset(_payload)

        stream.wait_for_offset(offset)


finally:
    stream.close()
"""
