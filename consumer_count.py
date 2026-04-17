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

print("Rozpoczynam zliczanie transakcji per sklep...")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    

    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1
    

    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (Przetworzono wiadomości: {msg_count}) ---")
        print(f"{'Sklep':<12} | {'Liczba TX':<10} | {'Suma PLN':<12}")
        print("-" * 40)
        
        # Iterujemy po wszystkich sklepach i wyświetlamy ich statystyki
        for s, count in store_counts.items():
            print(f"{s:<12} | {count:<10} | {total_amount[s]:>10.2f}")
