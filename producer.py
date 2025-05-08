import time
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# -------------------------------
# Kafka & Schema Registry Config
# -------------------------------
kafka_config = {
    'bootstrap.servers': "",               # Kafka broker address (Confluent Cloud or self-hosted)
    'sasl.mechanisms': 'PLAIN',            # Authentication mechanism
    'security.protocol': 'SASL_SSL',       # Security protocol
    'sasl.username': '',                   # Your Confluent Cloud Kafka API key
    'sasl.password': '',                   # Your Confluent Cloud Kafka API secret
    'group.id': 'group1',                  # Consumer group ID (not used by producer, included for completeness)
    'auto.offset.reset': 'latest'          # Reset policy if offset is invalid (again, for consumers)
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': '',  # Schema Registry endpoint URL
    'basic.auth.user.info': '{}:{}'.format('', '')  # API Key and Secret for Schema Registry
})

# ---------------------
# Avro Schema Handling
# ---------------------
subject_name = 'fed_x-value'  # Subject name in Schema Registry for the value schema
# Retrieve latest Avro schema string from the subject
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# ---------------------
# Serializers
# ---------------------
keyserializer = StringSerializer('utf8')  # Serialize string keys
avroSerializer = AvroSerializer(schema_registry_client, schema_str)  # Serialize values with Avro

# ---------------------
# Kafka Producer Setup
# ---------------------
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': keyserializer,
    'value.serializer': avroSerializer,
    "receive.message.max.bytes": 1000000000  # Optional: increase message size limit if needed
})

# ---------------------
# Delivery Report Callback
# ---------------------
def dileveryreport(err, msg):
    """Callback function to confirm message delivery status."""
    if err is not None:
        print('Delivery failed for user record {} with error: {}'.format(msg.key(), err))
    else:
        print('User record {} successfully published to topic [{}] partition [{}] at offset [{}]'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))
        print('===================')

# ---------------------
# Read and Process CSV
# ---------------------
df = pd.read_csv('')  # Provide the path to your CSV file

# Convert ISO 8601 timestamp to pandas datetime object
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

# Convert datetime to Unix timestamp in milliseconds (int64)
df['timestamp'] = df['timestamp'].astype('int64') // 10**6

# Loop through each row in DataFrame and send to Kafka
for index, row in df.iterrows():
    data = row  # This is a pandas Series; AvroSerializer expects a dict
    # You should convert to dictionary to match schema structure
    # Example: data = row.to_dict()

    producer.produce(
        topic='',  # Specify your Kafka topic here
        key=str(row['shipment_id']),  # Use shipment_id as message key
        value=data,  # Ensure this is a dict matching the Avro schema
        on_delivery=dileveryreport
    )
    
    producer.flush()  # Ensure message is sent before sleeping
    time.sleep(5)     # Delay to simulate streaming behavior