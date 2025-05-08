import time
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# kafka config
kafka_config = {
   'bootstrap.servers': "pkc-n3603.us-central1.gcp.confluent.cloud:9092",
   'sasl.mechanisms':'PLAIN',
   'security.protocol':'SASL_SSL',
   'sasl.username':'PP4RNK3JEQ5SZOMQ',
   'sasl.password':'eueQBZkPvTpXCYlEPNUTcp4oYLfjnPq9gtDhqaFD6Mv8uN5a8RQioC/rZ6mRChuo',
   'group.id':'group1',
   'auto.offset.reset':'latest'

}
schema_registry_client = SchemaRegistryClient({
    'url':'https://psrc-q8w9z6.us-central1.gcp.confluent.cloud',
    'basic.auth.user.info':'{}:{}'.format('TAHPJEWT4OZYMOHA','DJua0ID/4CtlaKKteal8gJNstUQYzZH35maz+bKSeQRlLCGU7bhjJS4j/WSpJ774')
})
subject_name = 'fed_x-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
keyserializer = StringSerializer('utf8')
avroSerializer = AvroSerializer(schema_registry_client,schema_str)
producer  = SerializingProducer({
    'bootstrap.servers' : kafka_config['bootstrap.servers'],
    'sasl.mechanisms':kafka_config['sasl.mechanisms'],
    'security.protocol':kafka_config['security.protocol'],
    'sasl.username' : kafka_config['sasl.username'],
    'sasl.password':kafka_config['sasl.password'],
    'key.serializer':keyserializer,
    'value.serializer': avroSerializer,
    "receive.message.max.bytes": 1000000000
}) 
def dileveryreport(err,msg):
    if err is not None:
        print('dilevery failed for user record {} with err {}'.format(msg.key,err))
        return
    print('User record {} successfully published to {} in partition{} at offset{}'.format(msg.key,msg.topic(),msg.partition(),msg.offset()))
    print('===================')


df = pd.read_csv('fedx_logistic_data.csv')
# Convert ISO timestamp string to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
# Convert to integer UNIX timestamp in milliseconds
df['timestamp'] = df['timestamp'].astype('int64') // 10**6
# df = df.fillna("null")
for index, row in df.iterrows():
    data = row
    # data = row.to_dict()

    producer.produce(
        topic='fed_x',
        key = str(row['shipment_id']),
        value = data,
        on_delivery = dileveryreport
    )
    producer.flush()
    time.sleep(5)




