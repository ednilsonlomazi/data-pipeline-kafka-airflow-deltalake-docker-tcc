import os
from ingestion.consumer import TraficConsumer
from multiprocessing import Process

# Busca o endereço do Kafka via variável de ambiente (definida no docker-compose)
# Se não encontrar, usa 'kafka:29092' como padrão para a rede interna do Docker
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# O caminho eh o ponto de montagem INTERNO do container
# mapeado no arquivo do compose, que eh um bind a estrutura de pastas do datalakehouse
pasta_raiz = '/app/datalakehouse/raw'

tc = TraficConsumer(bootstrap_servers=KAFKA_BROKER)

topicos = ['l01', 'l03', 'l06']
processos_consumo = []

for t in topicos:
    p = Process(target=tc.run_consumer, args=(t, pasta_raiz))
    processos_consumo.append(p)
    p.start()

for p in processos_consumo:
    p.join()