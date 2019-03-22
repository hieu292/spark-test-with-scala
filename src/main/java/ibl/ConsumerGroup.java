package ibl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerGroup {
    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.out.println("Usage: consumer <topic> <groupname>");
            return;
        }

        Logger logger = LoggerFactory.getLogger(ConsumerGroup.class.getName());
        String bootstrapServer = "localhost:9092";
        String topic = args[0].toString();
        String group = args[1].toString();

        final CountDownLatch latch = new CountDownLatch(3);


        for (int i = 0; i < 3; i++) {
            ConsumerRunnable consumerRunnable = new ConsumerRunnable(i, bootstrapServer, topic, group, latch);
            consumerRunnable.start();

            int finalI = i;

            // add a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Caught shutdown hook");
                consumerRunnable.shutdown();
                logger.info("Application has exited in thread " + finalI);
            }));
        }

        try {
            latch.await();
            logger.info("Application is starting");
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }

    }
}
