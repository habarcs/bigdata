# Supply Chain Management System
This project proposes a system for the efficient processing of high-volume supply chain data, with a primary focus on streaming order data, by utilizing an appropriate integration of advanced big data technologies. The system is capable of delivering real-time key performance indicators, accurate demand forecasts, and comprehensive analytical insights, thereby enabling informed decision-making and operational optimization.

## Implementation details
Our system leverages several big data technologies such as Apache Kafka for real-time messaging and data streaming, Apache Flink for distributed processing, Apache Spark and Facebook Prophet for machine learning, PostgreSQL for data storage, and Docker Compose for containerized orchestration. Each stage in the pipeline is modular, enabling seamless integration and scalability.

![System Architecture](https://github.com/habarcs/bigdata/blob/master/System%20Architecture.png)

For more information about the specific components and their functionality, please refer to the [report](https://github.com/habarcs/bigdata/blob/master/BigDataReport.pdf).

## Requirements
- docker 
- docker compose

## Run
To start the application all the user has to do is build the containers and run them in docker compose, the following command does both:

```bash
docker compose up -d --build
```

To connect to psql and interact with postgresql (the name of the container may be different):

```bash
docker exec -it supply_chain_big_data-sql-database-1 psql -U postgres
```

To connect to kafka and interact with the message queues (the name of the container may be different):
```bash
docker exec -it supply_chain_big_data-kafka-1 bash
```
The relevant programs are found in the /opt/kafka/bin/ directory
For example to list the messages in the order topic the user should run the following command:
```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders
```

## Use of the graphical user interface
The final dashboard is available on localhost:8501 after the setup has finished.
Here the user is able to monitor key performance indicators (KPIs) and visual analytics for inventory, sales, and delivery performance. Additionally, the dashboard provides demand forecasts derived from predictive models, enabling informed decision-making. Real-time streaming capabilities allow for dynamic visualization of changes in sales, ensuring up-to-date monitoring and insights.
