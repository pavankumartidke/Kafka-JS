const { Kafka } = require("kafkajs");

const topic = process.argv[2];
const groupId = process.argv[3];

// const TOPIC = "driver-positions-kstream";

function init() {
  const kafka = new Kafka({
    clientId: "kafka-streams-demo-client",
    brokers: ["localhost:9094"],
  });
  const consumer = kafka.consumer({ groupId });
  consumer.connect();

  consumer.subscribe({ topic: topic, fromBeginning: true });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`RECV::> Key: ${message.key.toString()} Value: ${message.value.toString()}`)
    },
  });
}

init();
