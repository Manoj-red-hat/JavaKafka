import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * Producer class
 *
 * Author : Manoj Kumar
 * Contact : manoj.kumar.mbm@gmail.com
 */
class KafkaProducer {
    private Properties configuration;
    private String topic;
    private Producer<String, String> pro;
    KafkaProducer(String topic,Properties conf){
        configuration=conf;
        this.topic=topic;
        pro = new org.apache.kafka.clients.producer.KafkaProducer<>(configuration);
    }

    void sentMsg() {
        Scanner in = new Scanner(System.in);
        String line = in.nextLine();
        while(!line.equals("exit")) {
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic, line);
            pro.send(rec);
            line = in.nextLine();
        }
    }
}
