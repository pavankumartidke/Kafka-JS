const { Kafka } = require("kafkajs");
const { KafkaStreams } = require("kafka-streams");

// configuration
const config = {
  noptions: {
    "metadata.broker.list": "localhost:9094",
    "group.id": "kafka-streams-demo",

  }
};

// client for producer
exports.Client = new Kafka({
  clientId: 'kafka-streams-demo-client',
  brokers: 'localhost:9094',
});

// client for kafka stream creation
exports.StreamClient = new KafkaStreams(config);
