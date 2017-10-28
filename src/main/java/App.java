import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String topic="a";
        Properties producerConf= new Properties();
        producerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.0.4.113:9092");
        producerConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        producerConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        Properties consumerConf = new Properties();
        consumerConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.0.4.113:9092");
        consumerConf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerConf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerConf.put(ConsumerConfig.GROUP_ID_CONFIG,"a");

        Runnable task=()->{
            try {
                KafkaConsumer cons=new KafkaConsumer(topic,consumerConf);
                cons.readMsg();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
        Thread t = new Thread(task);
        t.start();
        KafkaProducer pro=new KafkaProducer(topic,producerConf);
    }
}
