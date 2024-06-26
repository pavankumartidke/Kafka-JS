const fs = require("fs");
const path = require("path");
const { Kafka } = require("kafkajs");
const { SchemaRegistry, SchemaType } = require("@kafkajs/confluent-schema-registry");

// configurations
const config = {
  "bootstrap.servers": "localhost:9094",
  "schema.registry.url": "http://localhost:8081",
  'partitioner': 'murmur2_random'
};

// constants
const TOPIC = "driver-positions-kstream";
const SCHEMA_FILE_PREFIX = "../common_schemas/";
const DRIVER_FILE_PREFIX = "../drivers/";
const DRIVER_ID = "driver-3";
let pos = 0;

// logging
console.log("Starting AVRO Producer with Schema Registry...");

// Schema Registry client
const registry = new SchemaRegistry({ host: config["schema.registry.url"] });

// Load AVRO schemas and register if not already registered
const loadAndRegisterSchema = async (filePath) => {
  const schema = JSON.parse(fs.readFileSync(path.join(SCHEMA_FILE_PREFIX, filePath), "utf-8"));
  const { id } = await registry.register({
    type: SchemaType.AVRO,
    schema: JSON.stringify(schema),
  });
  return id;
};

// Kafka client
const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: [config["bootstrap.servers"]],
  
});

// Kafka producer
const producer = kafka.producer();

const run = async () => {
  await producer.connect();
  console.log("Producer is ready...");

  const keySchemaId = await loadAndRegisterSchema("position_key.avsc");
  const valueSchemaId = await loadAndRegisterSchema("position_value.avsc");

  console.log(`Key Schema ID: ${keySchemaId}, Value Schema ID: ${valueSchemaId}`);

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

    const line = lines[pos];
    const [latitude, longitude] = line.split(",").map(Number);

    const key = { key: DRIVER_ID };
    const value = { latitude, longitude };

    try {
      const encodedKey = await registry.encode(keySchemaId, key);
      const encodedValue = await registry.encode(valueSchemaId, value);

      // Produce message every sec
      await producer.send({
        topic: TOPIC,
        messages: [
          { key: encodedKey, value: encodedValue },
        ],
      });

      console.log(`SEND::> Key: ${JSON.stringify(key)} Value: ${JSON.stringify(value)}`);
    } catch (error) {
      console.error("Error encoding message:", error);
      exitHandler();
    }

    // other local updates
    pos += 1;
    setTimeout(sendMessage, 1000);
  };

  sendMessage();
};

// handle exit
// const exitHandler = async () => {
//   console.log("Flushing producer and exiting...");

//   try {
//     await producer.disconnect();
//     console.log("Producer disconnected.");
//   } catch (error) {
//     console.error("Error during disconnection:", error);
//   }
// };

const exitHandler = () => {
  console.log('Flushing producer and exiting.');
  producer.flush(10000, () => {
    console.log('Producer flushed and disconnected.');
    producer.disconnect();
  });
};

process.on("exit", exitHandler);
process.on("SIGINT", exitHandler);
process.on("SIGTERM", exitHandler);

run().catch(console.error);
