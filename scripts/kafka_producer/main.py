from ingestion.producer import TraficProducer
import json
from multiprocessing import Process
import os

# Busca o endereço do Kafka via variável de ambiente (definida no docker-compose)
# Se não encontrar, usa 'kafka:29092' como padrão para a rede interna do Docker
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

tp = TraficProducer(bootstrap_servers=KAFKA_BROKER)

arquivos_config = [
        {"file": '/app/data/volume_pedagiado_2025/VOLUME_PEDAGIADO-L01-HORARIO-2025.csv', "topic": "l01"},
        {"file": '/app/data/volume_pedagiado_2025/VOLUME_PEDAGIADO-L03-HORARIO-2025.csv', "topic": "l03"},
        {"file": '/app/data/volume_pedagiado_2025/VOLUME_PEDAGIADO-L06-HORARIO-2025.csv', "topic": "l06"}
    ]
processos = []

for item in arquivos_config:
    # Um processo para cada arquivo
    p = Process(target=tp.run_producer, args=(item['file'], item['topic'], 1000, 30))
    processos.append(p)
    p.start() # Inicia a execução em paralelo

# Aguarda todos os processos terminarem
for p in processos:
    p.join()

print("Todos os arquivos foram processados!")

