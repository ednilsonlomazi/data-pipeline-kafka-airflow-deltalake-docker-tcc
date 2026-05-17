import pandas as pd 
import time
import json
from confluent_kafka import Producer
import uuid
from datetime import datetime, timedelta

class KafkaDataProducer:
    def __init__(self, bootstrap_servers='kafka:29092'):
        self.bootstrap_servers = bootstrap_servers

    def delivery_report(self, err, msg):
        """Callback executado quando a mensagem é entregue ou falha."""
        if err is not None:
            print(f"Erro ao entregar: {err}")

    def run_producer(self, file_path, topic_name, chunk_size, wait_sec):
        """Método que será executado por cada processo"""
        print(f"Iniciando envio do arquivo: {file_path} para o tópico: {topic_name}")
        
        conf = {'bootstrap.servers': self.bootstrap_servers}
        producer = Producer(conf)
        
        
        df_iterator = pd.read_csv(file_path, sep=';', chunksize=chunk_size, dtype=str)

        for chunk in df_iterator:
            # Preenche nulos com vazio para não quebrar o parser do JSON
            chunk = chunk.fillna("")
            
            for row in chunk.itertuples(index=False):
                row_as_dict = row._asdict()
                
                # Injetando metadados para controle no Data Lakehouse
                row_as_dict["id_msn"] = str(uuid.uuid4())
                fuso_brasil = datetime.now() - timedelta(hours=3)
                row_as_dict["timestamp_ingestao"] = fuso_brasil.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                row_as_dict["datastamp"] = fuso_brasil.strftime('%Y-%m-%d')
                
                row_js = json.dumps(row_as_dict, ensure_ascii=False)

                producer.produce(
                    topic=topic_name, 
                    value=row_js.encode('utf-8'),
                    callback=self.delivery_report
                )
                producer.poll(0)
                
                # Simula o tempo real (streaming)
                time.sleep(wait_sec)
            
            # Garante o envio do chunk completo
            producer.flush()
