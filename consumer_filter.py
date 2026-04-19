from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def classify_risk(amount):
    if amount > 3000:
        return "HIGH"
    elif amount > 1000:
        return "MEDIUM"
    else:
        return "LOW"

for message in consumer:
    tx = message.value
    tx['risk_level'] = classify_risk(tx['amount'])

    if tx['risk_level'] == "HIGH":
        print(f"  HIGH   | {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")
    elif tx['risk_level'] == "MEDIUM":
        print(f"  MEDIUM | {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")
    else:
        print(f"  LOW    | {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")
