import requests
import json

ksql_server_url = "http://localhost:8088/ksql"

ksql_command = """
-- Define the state stream
CREATE STREAM state_stream (id VARCHAR KEY, state_info VARCHAR)
WITH (KAFKA_TOPIC='state_topic', VALUE_FORMAT='JSON');

-- Define the event stream
CREATE STREAM event_stream (id VARCHAR KEY, event_info VARCHAR)
WITH (KAFKA_TOPIC='event_topic', VALUE_FORMAT='JSON');

-- Create a table from the state stream
CREATE TABLE state_table AS
SELECT id, LATEST_BY_OFFSET(state_info) AS current_state
FROM state_stream
GROUP BY id
EMIT CHANGES;

-- Create a stream for processing updates
INSERT INTO state_stream
SELECT e.id as id, s.current_state+ e.event_info AS state_info
FROM event_stream e
LEFT JOIN state_table s ON e.id = s.id;

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