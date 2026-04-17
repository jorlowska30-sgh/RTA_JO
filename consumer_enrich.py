from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Uruchamiam proces wzbogacania danych (Enrichment)...")

for message in consumer:
    tx = message.value
    
    if tx['amount'] > 3000:
        tx['risk_level'] = "HIGH"
    elif tx['amount'] > 1000:
        tx['risk_level'] = "MEDIUM"
    else:
        tx['risk_level'] = "LOW"
        
    print(f"[{tx['risk_level']:^6}] TX_ID: {tx['tx_id']} | Kwota: {tx['amount']:>7.2f} PLN | Sklep: {tx['store']}")
