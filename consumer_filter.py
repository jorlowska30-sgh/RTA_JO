from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję tematu 'transactions' w poszukiwaniu dużych kwot...")

for message in consumer:
   
    tx = message.value
    
    if tx['amount'] > 1000:
        print(f"🚨 ALERT! Znaleziono dużą transakcję: ID={tx['tx_id']}, Kwota={tx['amount']:.2f} PLN, Sklep={tx['store']}")
