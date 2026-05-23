import os
from ingestion.consumer import SparkRawIngestion

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

def main():
    # Instancia a classe apenas UMA vez
    ingestion = SparkRawIngestion(bootstrap_servers=KAFKA_BROKER)
    
    topicos = ['l01', 'l03']
    queries = []

    for t in topicos:
        print(f"Configurando stream para o tópico: {t}")
        # Chamamos um método que inicia o stream mas NÃO usa awaitTermination() ainda
        query = ingestion.process_topic_to_raw(t, wait=False)
        queries.append(query)

    print("Todos os streams estão rodando em paralelo...")
   
    # Em vez de awaitAnyTermination(), fazemos isso:
    for query in queries:
        try:
            # Isso faz o Python esperar a query X terminar antes de olhar a Y
            # Mas como elas já estão rodando em paralelo no Spark, 
            # o Python só fica "travado" vigiando cada uma.
            query.awaitTermination()
        except Exception as e:
            print(f"A query do tópico {query.name} falhou: {e}")
            continue 

if __name__ == "__main__":
    main()