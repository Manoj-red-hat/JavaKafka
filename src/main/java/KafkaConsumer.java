import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by manoj.kumar1 on 10/24/2017.
 */
public class KafkaConsumer {
    Properties configuration;
    String topic;
    Consumer consumer;

    KafkaConsumer(String topic,Properties conf) throws InterruptedException {
        configuration = conf;
        this.topic = topic;
        initializeConsumer();
    }

    private void initializeConsumer() {
        consumer=new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(configuration);
        consumer.subscribe(Collections.singletonList("a"));
    }

    public void readMsg()throws InterruptedException {

        final int giveUp = 10000;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
}
