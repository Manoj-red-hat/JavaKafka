import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 *KafkaProducerConsumer
 *
 * This class will initiate Consumer and Producer for Kafka broker
 *
 * Author : Manoj Kumar
 * Contact : manoj.kumar.mbm@gmail.com
 */
public class KafkaProducerConsumer
{
    public static void main( String[] args )
    {
        //Topic on which consumer subscribe and producer produce
        final String topic;
        topic = "a";

        //Kafka bootstrap server
        final String BootStrapServer;
        BootStrapServer = "10.0.4.113:9092";

        //Refer :- https://www.tutorialspoint.com/apache_kafka/apache_kafka_consumer_group_example.htm
        final String groupId;
        groupId = "testGroup";

        //Producer properties
        Properties producerConf= new Properties();
        producerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootStrapServer);
        producerConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        producerConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //Consumer properties
        Properties consumerConf = new Properties();
        consumerConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootStrapServer);
        consumerConf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerConf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerConf.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        /*
        Below task is responsible for communicating to kafka broker and read data on topic
         */
        Runnable consumerTask = () -> {
            try {
                KafkaConsumer cons=new KafkaConsumer(topic,consumerConf);
                cons.readMsg();
            } catch (InterruptedException e) {
                System.out.println("Error in creating consumer, enable debug logs from log properties for debug");
                e.printStackTrace();
                System.exit(1);
            }
        };
        /*
        Create and initialize consumer thread
         */
        Thread t = new Thread(consumerTask);
        t.start();
        /*
        Producer which is going to write data on topic
         */
        KafkaProducer pro=new KafkaProducer(topic,producerConf);
        pro.sentMsg();
    }
}
