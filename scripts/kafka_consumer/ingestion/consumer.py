import json
import os
import time
from confluent_kafka import Consumer, KafkaError
from multiprocessing import Process

class TraficConsumer:

    def __init__(self, bootstrap_servers='kafka:29092'):
        self.bootstrap_servers = bootstrap_servers

    def run_consumer(self, topic_name, output_folder):
        """Método que será executado por cada processo de consumo"""
        
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f'grupo-{topic_name}', 
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }

        consumer = Consumer(conf)
        consumer.subscribe([topic_name])

        path = os.path.join(output_folder, topic_name)
        if not os.path.exists(path):
            os.makedirs(path)

        print(f"Processo iniciado: Consumindo {topic_name}...")

        try:
            while True:
                # por 1 segundo, tenta ler uma mensagem
                msg = consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    # se leu todas as mensagem e chegou ao fim, continua, senao eh um erro fatal
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Erro Fatal no {topic_name}: {msg.error()}")
                        break

                try:
                    # decodifica os bytes (json) em string e armazena e converte para dict
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Define nome do arquivo baseado em milisegundos (UniX Epock) e salva
                    filename = f"msg_{int(time.time() * 1000)}.json"
                    file_path = os.path.join(path, filename)
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False)

                except Exception as e:
                    print(f"Erro ao processar no {topic_name}: {e}")

        except KeyboardInterrupt:
            print(f"Parando consumidor do {topic_name}...")
        finally:
            consumer.close()