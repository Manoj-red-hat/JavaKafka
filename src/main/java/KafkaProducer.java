import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by manoj.kumar1 on 10/24/2017.
 */
public class KafkaProducer {
    Properties configuration;
    String topic;
    KafkaProducer(String topic,Properties conf){
        configuration=conf;
        this.topic=topic;
        initializeProducer();
    }

    private void initializeProducer(){
        Producer pro=new org.apache.kafka.clients.producer.KafkaProducer<String,String>(configuration);
        Scanner in = new Scanner(System.in);
        String line = in.nextLine();
        while(!line.equals("exit")) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, line);
            pro.send(rec);
            line = in.nextLine();
        }
    }
}
