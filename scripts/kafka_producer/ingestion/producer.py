import pandas as pd 
import time
import json
from confluent_kafka import Producer
import uuid
from datetime import datetime, timedelta


class TraficProducer:
    def __init__(self, bootstrap_servers='kafka:29092'):
        self.bootstrap_servers = bootstrap_servers

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Erro: {err}")

    def run_producer(self, file_path, topic_name, chunk_size, wait_sec):
        """Método que será executado por cada processo"""
        print(f"Iniciando processamento do arquivo: {file_path}")
        
        conf = {'bootstrap.servers': self.bootstrap_servers}
        producer = Producer(conf) # Declarando o producer ----------------------------
        
        df_iterator = pd.read_csv(file_path, chunksize=chunk_size)

        for chunk in df_iterator:
            
            chunk.rename(columns={
                'DATA': 'data', 'HORA': 'hora', 'LOTE': 'num_lote',
                'PRACA': 'desc_praca', 'SENTIDO': 'desc_sentido',
                'TIPO_PISTA': 'desc_tipo_pista', 'TIPO_PASSAGEM': 'desc_tipo_passagem',
                'TP_PAGAMENTO': 'desc_tipo_pagamento'
            }, inplace=True)

            df_melted = chunk.melt(
                id_vars=['data', 'hora', 'num_lote', 'desc_praca', 'desc_sentido', 
                         'desc_tipo_pista', 'desc_tipo_passagem', 'desc_tipo_pagamento'],
                var_name='desc_tipo_veiculo',
                value_name='qtd_veiculos'
            )

            for row in df_melted.itertuples(index=False):
                quantidade = int(row.qtd_veiculos)
                
                for i in range(quantidade):
                    row_as_dict = row._asdict()
                    row_as_dict["id_msn"] = str(uuid.uuid4())
                    fuso_brasil = datetime.now() - timedelta(hours=3)
                    row_as_dict["timestamp"] = fuso_brasil.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    row_as_dict["datastamp"] = fuso_brasil.strftime('%Y-%m-%d')
                    row_js = json.dumps(row_as_dict, ensure_ascii=False)

                    producer.produce(
                        topic=topic_name, 
                        value=row_js.encode('utf-8'),
                        callback=self.delivery_report
                    )
                    producer.poll(0)
                    time.sleep(wait_sec)
            
            producer.flush()
        print(f"Finalizado: {file_path}")
        



            