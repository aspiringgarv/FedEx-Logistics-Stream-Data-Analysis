import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Constants
STATUSES = ["in-transit", "delivered", "pending", "cancelled"]
NUM_ROWS = 10000
CSV_FILENAME = "fedx_logistic_data.csv"

# Function to generate a random ISO 8601 timestamp
def random_timestamp(start_year=2022, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_date = start + timedelta(days=random.randint(0, delta.days),
                                    seconds=random.randint(0, 86400))
    return random_date.isoformat() + "Z"

# Generate mock data
rows = []
for i in range(1, NUM_ROWS + 1):
    shipment_id = f"SH{i:06d}"
    origin = fake.city() + ", " + fake.state_abbr()
    destination = fake.city() + ", " + fake.state_abbr()
    status = random.choice(STATUSES)
    timestamp = random_timestamp()
    rows.append([shipment_id, origin, destination, status, timestamp])

# Write to CSV
with open(CSV_FILENAME, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["shipment_id", "origin", "destination", "status", "timestamp"])
    writer.writerows(rows)

print(f"{NUM_ROWS} rows written to {CSV_FILENAME}")