from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"\n═══ Po {msg_count} wiadomościach ═══")
        print(f"{'Sklep':<12} {'Liczba':>8} {'Suma [PLN]':>14} {'Średnia [PLN]':>16}")
        print("─" * 52)
        for s in sorted(store_counts, key=store_counts.get, reverse=True):
            n = store_counts[s]
            total = total_amount[s]
            avg = total / n
            print(f"{s:<12} {n:>8} {total:>14.2f} {avg:>16.2f}")
        print(f"{'RAZEM':<12} {msg_count:>8} {sum(total_amount.values()):>14.2f}")
