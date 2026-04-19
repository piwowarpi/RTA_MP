from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def score_transaction(tx):
    score = 0
    rules = []
    
    # R1: wysoka kwota
    if tx['amount'] > 3000:
        score += 3
        rules.append('R1')
    
    # R2: droga elektronika
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append('R2')
    
    # R3: godzina nocna
    if tx.get('hour', 12) < 6:
        score += 2
        rules.append('R3')
    
    return score, rules

for message in consumer:
    tx = message.value
    
    score, rules = score_transaction(tx)
    
    if score >= 3:
        alert = {
            **tx,
            'score': score,
            'rules': rules,
        }
        
        alert_producer.send('alerts', value=alert)
        
        print(f" ALERT | score={score} rules={'+'.join(rules)} | "
              f"{tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['category']} | "
              f"h={tx.get('hour')} | {tx['store']}")
