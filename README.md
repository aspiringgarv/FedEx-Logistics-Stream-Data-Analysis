# 🚚 FedEx Logistics Data Streaming Pipeline

This project demonstrates a real-time data streaming pipeline using **Apache Kafka**, **MongoDB**, and a **custom dashboard** to visualize logistics data. It uses **Avro serialization** and **Confluent Cloud** for Kafka management.

---

## 📌 Goal

The goal of this assignment is to set up a data streaming pipeline using Kafka and MongoDB, and to create a dashboard for visualizing logistics data.

---

## 🏗️ Architecture

```text
CSV File
   ↓
Kafka Python Producer (Avro + Schema Registry)
   ↓
Kafka Topic (Confluent Cloud)
   ↓
MongoDB Sink Connector
   ↓
MongoDB Atlas
   ↓
Data Visualization Dashboard (e.g. Streamlit / Plotly / Dash)
```

---

## 📂 Dataset

Sample CSV data (`fedx_logistic_data.csv`):

| shipment_id | origin           | destination      | status    | timestamp               |
|-------------|------------------|------------------|-----------|--------------------------|
| SH000001    | Brownmouth, ND   | Holtmouth, WY    | delivered | 2024-08-26T15:07:41Z     |

- The `timestamp` field is converted to Unix epoch (milliseconds) before publishing.

---

## 🛠️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/fedex-logistics-streaming.git
cd fedex-logistics-streaming
```

### 2. Install Dependencies

```bash
pip install pandas confluent_kafka
```

---

## 🚀 Kafka Producer

### 🔐 Kafka & Schema Registry Config

Ensure your credentials are correct in `producer.py`:

```python
'bootstrap.servers': "<your_confluent_broker>",
'sasl.username': "<your_api_key>",
'sasl.password': "<your_api_secret>",
'basic.auth.user.info': "<schema_registry_key>:<schema_registry_secret>"
```

### ▶️ Run the Producer

```bash
python producer.py
```

This script:
- Reads `fedx_logistic_data.csv`
- Converts `timestamp` to Unix time (ms)
- Sends each record to Kafka using Avro
- Sleeps 5 seconds between records

---

## 🗄️ MongoDB Sink Connector

Set up a **MongoDB Kafka Connector** via Confluent Cloud or Kafka Connect:

### Sample Connector Config

```json
{
  "name": "fedex-mongo-connector",
  "connector.class": "MongoDbSink",
  "topics": "fed_ex_logistic_data",
  "connection.uri": "mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/test?retryWrites=true&w=majority",
  "database": "logistics",
  "collection": "shipments",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "https://<your_schema_registry_url>",
  "value.converter.basic.auth.credentials.source": "USER_INFO",
  "value.converter.basic.auth.user.info": "<schema_registry_key>:<schema_registry_secret>"
}
```

---

## 📊 Dashboard

A dashboard can be created using **Streamlit**, **Plotly Dash**, or **any Python-based tool** to visualize data from MongoDB also can also be created on Mongo_db.

You can display:
- Shipments by status
- Average delivery time
- Shipment trends over time

---

## 📄 Documentation

Detailed report: [Google Docs Link](https://docs.google.com/document/d/1RnzHiLM-jksoOq7natjI5hSwcRuCuMqJIFNQmObBDn4/edit?usp=sharing)

---
