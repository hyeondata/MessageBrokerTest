# MessageBrokerTest
MessageBrokerTest (Kafka, RabbitMq)

#rabbitMq test
docker-compose -f rabbitmq-compose.yml up -d
sanding code : rabbitmqSending.go
receving code : rabbitmqReceiving.go

#kafka Test
docker-compose -f kafka-compose.yml up -d
sanding code : kafkaSending.go
receving code : kafkaReceiving.go
