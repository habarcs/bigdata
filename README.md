# Demand Prediction for Supply Chain Management
The program provides an end-to-end solution for processing and analyzing order data with inventory management and demand fluctuation capabilities.
It is able to handle multiple retailers and products, handles the data of customers and processes and manages orders in a scalable manner.

## Implementation details
For more information about the specific components and their workings check the paper:
TODO add the link for the paper

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
docker exec -it best_big_data_project-sql-database-1 psql -U postgres
```

To connect to kafka and interact with the message queues (the name of the container may be different):
```bash
docker exec -it best_big_data_project-kafka-1 bash
```
The relevant programs are found in the /opt/kafka/bin/ directory
For example to list the messages in the order topic the user should run the following command:
```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders
```

## Use of the graphical user interface
The Main window is available on localhost:8501 after the setup has finished
Here the user is able to check the inventory for each retailer and look for historical and predicted future daily demands.