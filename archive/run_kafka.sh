docker run --name mykafka -p 9092:9092 --net abc  -e KAFKA_ADVERTISED_HOST_NAME=mykafka -e KAFKA_ZOOKEEPER_CONNECT=myzoo:2181  -v /var/run/docker.sock:/var/run/docker.sock  wurstmeister/kafka
