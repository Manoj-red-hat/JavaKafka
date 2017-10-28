import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Collections;
import java.util.Properties;

/**
 * Consumer class
 *
 * Author : Manoj Kumar
 * Contact : manoj.kumar.mbm@gmail.com
 */

class KafkaConsumer {
    private Properties configuration;
    private String topic;
    private Consumer consumer;

    KafkaConsumer(String topic,Properties conf) throws InterruptedException {
        configuration = conf;
        this.topic = topic;
        initializeConsumer();
    }

    private void initializeConsumer() {
        consumer=new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(configuration);
        consumer.subscribe(Collections.singletonList("a"));
    }

    void readMsg() throws InterruptedException {

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count() == 0) continue;

            consumerRecords.forEach(record ->
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset())
            );

            consumer.commitAsync();
        }
    }
}
