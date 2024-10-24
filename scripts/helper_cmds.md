# Docker - Compose

* To rebuild and start the docker services, run the following command:

```bash
docker-compose up -d
```

* You can access the ksqlDB command-line interface (CLI) using:

```bash
docker-compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

* To access the Kafka CLI, execute:

```bash
docker-compose exec kafka /bin/bash
```

* To consume messages from the `manufacturers` topic, use the command below:

```bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic manufacturers --from-beginning --max-messages 2
```
```
