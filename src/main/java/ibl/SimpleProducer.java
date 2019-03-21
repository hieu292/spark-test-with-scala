package ibl;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;


public class SimpleProducer {

    public static void main(String[] args) throws Exception{

        final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

        // Check arguments length value
        if(args.length == 0){
            System.out.println("Enter topic name");
            return;
        }

        //Assign topicName to string variable
        String topicName = args[0].toString();

        // create instance for properties to access producer configs
        Properties props = new Properties();

        String bootrapServer = "localhost:9092";

        //Assign localhost id
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootrapServer);

        //Set acknowledgements for producer requests.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        //If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        //Specify buffer size in config
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        //Reduce the no of requests less than 0
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        for(int i = 0; i < 100; i++){
            Random rand = new Random();
            String key = "id_" + rand.nextInt(4);
            String value = Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    topicName,
                    key,
                    value
            );

            // todo: Same key => will send in same partition

            logger.info("Key: " + key);

            // todo: send function is async function
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        // Record was sent successfully
                        logger.info("Received metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Record sent failure: " + exception);
                    }
                }
            }).get(); // todo: block the .send() to make it synchronous - don't do this in production!
        }

        logger.info("Message sent completed");
        producer.close();
    }
}

