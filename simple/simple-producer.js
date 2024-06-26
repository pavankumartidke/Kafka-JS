const fs = require("fs");
const path = require("path");
const { Kafka } = require("kafkajs");

const config = {
  "bootstrap.servers": "localhost:9094",
  "schema.registry.url": "http://localhost:8081",
  partitioner: "murmur2_random",
};

// constants
const TOPIC = "driver-positions";
const SCHEMA_FILE_PREFIX = "../common_schemas/";
const DRIVER_FILE_PREFIX = "../drivers/";
const DRIVER_ID = "driver-2";
let pos = 0;

// Kafka client
const kafka = new Kafka({
  clientId: "my-producer-s",
  brokers: [config["bootstrap.servers"]],
});

// Kafka producer
const producer = kafka.producer();

const run = async () => {
  await producer.connect();
  console.log("Producer is ready...");

  // loading dummy data file
  const driver_file = path.join(DRIVER_FILE_PREFIX, `${DRIVER_ID}.csv`);
  const lines = fs
    .readFileSync(driver_file, "utf-8")
    .split("\n")
    .filter(Boolean);

  const sendMessage = async () => {
    if (pos >= lines.length) {
      pos = 0;
    }

    const currentLine = lines[pos];
    const [latitude, longitude] = currentLine.split(","); // Split into strings without converting to numbers
  
    const key = DRIVER_ID; // Simple string key
    const value = `${latitude},${longitude}`; // Simple string value

    // Serialize the key and value to JSON strings and then to buffers
    const keyBuffer = Buffer.from(JSON.stringify(key));
    const valueBuffer = Buffer.from(value.replace('\r', ''));
    // const valueBuffer = Buffer.from(JSON.stringify(value.replace('\r', '')));

    try {
      // Produce message every second
      await producer.send({
        topic: TOPIC,
        messages: [{ key: keyBuffer, value: valueBuffer }],
      });

      console.log(
        `SEND:: Key: ${JSON.stringify(key)} Value: ${JSON.stringify(value)}`
      );
    } catch (error) {
      console.error("Error sending message:", error);
    }

    // other local updates
    pos += 1;
    setTimeout(sendMessage, 600);
  };

  sendMessage();
};

// handle exit
const exitHandler = () => {
  console.log("Flushing producer and exiting.");
  producer.flush(10000, () => {
    console.log("Producer flushed and disconnected.");
    producer.disconnect();
  });
};

process.on("exit", exitHandler);
process.on("SIGINT", exitHandler);
process.on("SIGTERM", exitHandler);

run().catch(console.error);
