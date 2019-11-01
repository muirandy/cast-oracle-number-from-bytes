# Hello!

## Useful Kafka commands:

### Show the topics:
```
docker exec -it CONTAINER_ID kafka-topics --list --zookeeper zookeeper:2181
```

### View the content of a topic (String):
```
docker exec -it CONTAINER_ID kafka-console-consumer --bootstrap-server broker:9092 --property print.key=true --topic input --from-beginning
docker exec -it CONTAINER_ID kafka-console-consumer --bootstrap-server broker:9092 --property print.key=true --topic output --from-beginning
```

### View the content of a topic (Avro):
```
docker run -it --rm --network=faith edenhill/kafkacat:1.5.0 kafkacat -b broker:29092 -o beginning -r http://schema-registry:8081 -s avro -t input
```