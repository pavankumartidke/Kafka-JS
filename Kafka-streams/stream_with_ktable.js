// const { KafkaStreams } = require("kafka-streams");
// // const { StreamClient } = require("./client");

// // constants
// const INPUT_KSTREAM_TOPIC = "driver-positions-kstream";
// const INPUT_KTABLE_TOPIC = "driver-positions-ktable";
// const OUTPUT_TOPIC = "driver-positions-kstream-out";


// // configuration
// const config = {
//   noptions: {
//     "metadata.broker.list": "localhost:9094",
//     "group.id": "kafka-streams-demo",
//   }
// };


// // Custom serializer and deserializer
// const customSerialize = (data) => Buffer.from(JSON.stringify(data));
// const customDeserialize = (data) => JSON.parse(data.toString());

// // stream client
// const StreamClient = new KafkaStreams(config);
// StreamClient.on("error", (error) => console.error(error));


// // create stream from topic
// const stream = StreamClient.getKStream(INPUT_KSTREAM_TOPIC);
// // const ktable = StreamClient.getKTable(INPUT_KTABLE_TOPIC);
 

// // ktable
// // .mapJSONConvenience()
// //   .start()
// //   .then(() => console.log("KTable Started..."))
// //   .catch(error => console.error("Error starting KTable:", error))


// const ktable = stream.map(value => {
//   const key = customDeserialize(message.key);
//   const value = customDeserialize(message.value);
//   return { key, value };
// })
// .gr



//   // stream
//   // .innerJoin(ktable, 'driverId', true, (data) => {
//   //   console.log('---===>> ', data);

//   //   return 'pavan';
//   // })
//   // .tap((message) => {
//   //     console.log("Processed message:", message);
//   // })
//   // .to(OUTPUT_TOPIC, "auto", "buffer");
  



    
// // start sttream processing
// stream
//   .start()
//   .then(() => {
//     console.log("Stream started.");
//   })
//   .catch((error) => {
//     console.error("Error starting stream:", error);
//   });

// process.on("SIGINT", () => {
//   console.log("Flushing and closing streams...");
//   StreamClient.closeAll();
//   process.exit();
// });



// =========================================================================================================================================

const {Kafka} = require('kafkajs');
const fs = require('fs');
const path = require('path');

const INPUT_KSTREAM_TOPIC = 'driver-positions-kstream';
const INPUT_KTABLE_TOPIC = 'driver-positions-ktable';
const OUTPUT_TOPIC = 'driver-positions-kstream-out';

const kafka = new Kafka({
  clientId: 'kafka-streams-demo-client',
  brokers: ['localhost:9094']
});

const consumer = kafka.consumer({ groupId: 'kafka-streams-demo' });
const producer = kafka.producer();

const customSerialize = (data) => JSON.stringify(data);
const customDeserialize = (data) => JSON.parse(data);

const state = {};

const calculateDistance = (lat1, lon1, lat2, lon2) => {
  const R = 6371e3; // Earth radius in meters
  const φ1 = (lat1 * Math.PI) / 180;
  const φ2 = (lat2 * Math.PI) / 180;
  const Δφ = ((lat2 - lat1) * Math.PI) / 180;
  const Δλ = ((lon1 - lon2) * Math.PI) / 180;

  const a =
    Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
    Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c;
};

const processMessage = async (message) => {
  const key = customDeserialize(message.key);
  const value = customDeserialize(message.value);

  if (!state[key]) {
    state[key] = { latitude: value.latitude, longitude: value.longitude, distance: 0.0 };
  } else {
    const { latitude: aggLatitude, longitude: aggLongitude, distance: aggDistance } = state[key];
    const distance = calculateDistance(aggLatitude, aggLongitude, value.latitude, value.longitude);
    state[key] = {
      latitude: value.latitude,
      longitude: value.longitude,
      distance: aggDistance + 'pavan'
    };
  }

  const updatedValue = state[key];
  await producer.send({
    topic: OUTPUT_TOPIC,
    messages: [
      {
        key: customSerialize(key),
        value: customSerialize(updatedValue)
      }
    ]
  });
};

const run = async () => {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: INPUT_KSTREAM_TOPIC, fromBeginning: true });
  await consumer.subscribe({ topic: INPUT_KTABLE_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        await processMessage(message);
      } catch (error) {
        console.error('Error processing message:', error);
      }
    }
  });

  process.on('SIGINT', async () => {
    console.log('Disconnecting...');
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  });
};

run().catch(console.error);
