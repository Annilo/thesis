import requests
import json

ksql_server_url = "http://localhost:8088/ksql"

ksql_command = """
CREATE TABLE IF NOT EXISTS xes_model_test (
    node_id VARCHAR PRIMARY KEY,
    label VARCHAR,
    parent VARCHAR,
    children ARRAY<VARCHAR>
) WITH (
    KAFKA_TOPIC = 'test_xes_topic',
    VALUE_FORMAT = 'JSON'
);
CREATE TABLE IF NOT EXISTS event_labels (
    event VARCHAR PRIMARY KEY,
    label VARCHAR
) WITH (
    KAFKA_TOPIC = 'test_label_topic',
    VALUE_FORMAT = 'JSON'
);
CREATE STREAM IF NOT EXISTS raw_event_data (
    event STRUCT<`concept:name` STRING, `time:timestamp` STRING>,
    trace STRUCT<`concept:name` STRING>
) WITH (
    KAFKA_TOPIC = 'test_event_data',
    VALUE_FORMAT = 'JSON'
);
CREATE STREAM IF NOT EXISTS event_data AS 
SELECT 
    event->`concept:name` AS activity, 
    event->`time:timestamp` AS timestamp, 
    trace->`concept:name` AS trace
FROM 
    raw_event_data 
EMIT CHANGES;
-- here i would need the logic of a table which keeps the last event type (A,B,C etc),last node id, the cost already and trace id
"""

headers = {
    'Content-Type': 'application/vnd.ksql.v1+json',
    'Accept': 'application/vnd.ksql.v1+json'
}


data = {
    "ksql": ksql_command,
    "streamsProperties": {}
}

# Send the request to KSQL server
response = requests.post(ksql_server_url, headers=headers, data=json.dumps(data))
print(response.text)