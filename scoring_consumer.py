from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []
    

    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append('R1 (High Amount)')
        

    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append('R2 (Expensive Tech)')

    tx_hour = tx.get('hour')
    if tx_hour is None and 'timestamp' in tx:
        tx_hour = datetime.fromisoformat(tx['timestamp']).hour
        
    if tx_hour is not None and tx_hour < 6:
        score += 2
        rules.append('R3 (Night Hours)')
        
    return score, rules

print("Silnik scoringowy uruchomiony. Analizuję transakcje...")

for message in consumer:
    tx = message.value
    score, triggered_rules = score_transaction(tx)
    

    if score >= 3:

        tx['fraud_score'] = score
        tx['triggered_rules'] = triggered_rules
        
     
        alert_producer.send('alerts', value=tx)
        
        print(f"🚨 ALERT: Wykryto podejrzaną transakcję!")
        print(f"   ID: {tx['tx_id']} | Punkty: {score} | Reguły: {triggered_rules}")
        print(f"   Szczegóły: {tx['amount']} PLN | {tx['category']} | Godzina: {tx.get('hour', 'Nieznana')}")
        print("-" * 30)


alert_producer.flush()
