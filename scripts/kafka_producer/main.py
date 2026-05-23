from ingestion.producer import KafkaDataProducer
import os
from multiprocessing import Process

# Busca o endereço do Kafka via variável de ambiente (definida no docker-compose)
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

def main():
    tp = KafkaDataProducer(bootstrap_servers=KAFKA_BROKER)

    
    arquivos_config = [
            {"file": '/app/data/financas/ft_receita.csv', "topic": "l01"},
            {"file": '/app/data/financas/ft_divida_pub.csv', "topic": "l03"}
        ]
    
    processos = []

    for item in arquivos_config:
        # Passando wait_sec=3 para injetar 1 mensagens a cada 3 segundo por tópico
        p = Process(target=tp.run_producer, args=(item['file'], item['topic'], 500, 30))
        processos.append(p)
        p.start() # Inicia a execução em paralelo

    # Aguarda todos os processos terminarem
    for p in processos:
        p.join()

    print("Todos os dados financeiros foram enviados para o Kafka!")

if __name__ == "__main__":
    main()