package ibl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    private Thread t;
    private Integer threadId;
    private CountDownLatch latch;
    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
    private Duration durationTime = Duration.ofMillis(100);
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(Integer id, String bootstrapServer, String topic, String group, CountDownLatch lt){
        threadId = id;
        latch = lt;

        // Create Consumer configs

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topic));
        logger.info("Subscribed to topic: " + topic);
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(durationTime);
                for (ConsumerRecord<String, String> record : records){
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
            }
        } catch (WakeupException e){
            logger.info("Received shutdown signal!");
            consumer.close();
        }
    }

    public void start () {
        System.out.println("Starting " +  threadId );
        latch.countDown();
        if (t == null) {
            t = new Thread(this, Integer.toString(threadId));
            t.start ();
        }
    }

    public void shutdown() {
        // the wakeup() method is a special method to interrupt consumer.poll()
        // it will throw the exception WakeUpException
        consumer.wakeup();
    }
}
