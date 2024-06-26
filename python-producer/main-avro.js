const fs = require('fs');
const path = require('path');
// const { Kafka, logLevel } = require('node-rdkafka');
const Kafka = require('node-rdkafka');
const avro = require('avsc');

// Constants
const DRIVER_FILE_PREFIX = "./";
const KAFKA_TOPIC = "driver-positions-pyavro";
const DRIVER_ID = "driver-2";

// Logging
console.log("Starting Node.js Avro producer.");

// Load Avro schemas
const keySchema = avro.Type.forSchema(JSON.parse(fs.readFileSync("position_key.avsc", 'utf8')));
const valueSchema = avro.Type.forSchema(JSON.parse(fs.readFileSync("position_value.avsc", 'utf8')));

// Kafka producer configuration
const producer = new Kafka.Producer({
  'bootstrap.servers': 'localhost:9094',
  // 'dr_cb': true,
  // 'client.id': 'node-avro-producer',
  // 'log_level': logLevel.INFO,
});

// Connect the producer
producer.connect();

// Handle ready event
producer.on('ready', () => {
  console.log('Producer is ready');

  const driverFile = path.join(DRIVER_FILE_PREFIX, `${DRIVER_ID}.csv`);
  const lines = fs.readFileSync(driverFile, 'utf8').split('\n').filter(Boolean);
  let pos = 0;

  const sendMessage = () => {
    if (pos >= lines.length) {
      pos = 0;
    }

    const line = lines[pos];
    const [latitude, longitude] = line.split(',').map(Number);

    const key = { key: DRIVER_ID };
    const value = { latitude, longitude };

    producer.produce(
      KAFKA_TOPIC,
      null,
      valueSchema.toBuffer(value),
      keySchema.toBuffer(key),
      Date.now(),
      (err, offset) => {
        if (err) {
          console.error('Error producing message:', err);
        } else {
          console.log(`Sent Key: ${JSON.stringify(key)} Value: ${JSON.stringify(value)}`);
        }
      }
    );

    pos += 1;
    setTimeout(sendMessage, 1000);
  };

  sendMessage();
});

// Handle delivery report
producer.on('delivery-report', (err, report) => {
  if (err) {
    console.error('Delivery report error:', err);
  } else {
    console.log('Delivery report:', report);
  }
});

// Handle disconnect event
producer.on('disconnected', () => {
  console.log('Producer has disconnected');
});

// Handle exit
const exitHandler = () => {
  console.log('Flushing producer and exiting.');
  producer.flush(10000, () => {
    console.log('Producer flushed and disconnected.');
    producer.disconnect();
  });
};

process.on('exit', exitHandler);
process.on('SIGINT', exitHandler);
process.on('SIGTERM', exitHandler);
