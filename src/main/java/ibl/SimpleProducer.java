package ibl;

//import util.properties packages

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

//import simple producer packages
//import KafkaProducer packages
//import ProducerRecord packages

//Create java class named “SimpleProducer”
public class SimpleProducer {

    public static void main(String[] args) throws Exception{

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

        for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();
    }
}

