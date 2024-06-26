const { KafkaStreams } = require("kafka-streams");

// constants
const INPUT_TOPIC = "driver-positions-kstream";
const OUTPUT_TOPIC = "driver-positions-kstream-out";


// configuration
const config = {
  noptions: {
    "metadata.broker.list": "localhost:9094",
    "group.id": "kafka-streams-demo",
  }``
};

const StreamClient = new KafkaStreams(config);
StreamClient.on("error", (error) => console.error(error));

// create stream from topic
const stream = StreamClient.getKStream(INPUT_TOPIC);


stream
    .mapJSONConvenience()  // Convert messages to JSON
    .map((message) => {
        // Transform the message
        const transformedMessage = {
          ...message.value,
          processedAt: new Date().toISOString()
        };
        return transformedMessage;
    })
    .tap((message) => {
        console.log("Processed message:", message);
    })
    .to(OUTPUT_TOPIC, "auto", "buffer");  // Produce to another topic


    
// start sttream processing
stream
  .start()
  .then(() => {
    console.log("Stream started.");
  })
  .catch((error) => {
    console.error("Error starting stream:", error);
  });

process.on("SIGINT", () => {
  console.log("Flushing and closing streams...");
  StreamClient.closeAll();
  process.exit();
});
