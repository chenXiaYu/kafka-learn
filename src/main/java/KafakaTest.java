import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KafakaTest {

    private static Properties produceProperties;
    private static Properties consumerProperties;
    private static String kafkaHost;
    private static String groupId;

    private static String topic ;
    static {
        topic = "heima";
        kafkaHost = "192.168.92.130:9092";
        groupId = "group.demo";
        produceProperties = new Properties();
        produceProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaHost);
        produceProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        produceProperties.setProperty(ProducerConfig.RETRIES_CONFIG,"2");
        produceProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaHost);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
    }


    /**
     * 生产者
     */
    @Test
    public void testSend(){
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(produceProperties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,"kafa-demo-1","hello,kafka!");
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();

    }

    /**
     * 生产者 同步发送
     */
    @Test
    public void testSendSync() throws ExecutionException, InterruptedException {
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(produceProperties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,"kafa-demo-1","hello,kafka!");
        RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
        //失败则会抛出异常
        System.out.println("topic:"+recordMetadata.topic().toString());
        System.out.println("partition:"+recordMetadata.partition());
        System.out.println("offset:"+recordMetadata.offset());

        kafkaProducer.close();

    }

    /**
     * 生产者 异步发送
     */
    @Test
    public void testSendASync() throws ExecutionException, InterruptedException {
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(produceProperties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,"kafa-demo-1","hello,kafka!");

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println("topic:"+recordMetadata.topic().toString());
                System.out.println("partition:"+recordMetadata.partition());
                System.out.println("offset:"+recordMetadata.offset());
            }
        });

        kafkaProducer.close();

    }





    @Test
    public void testConsumer(){
        KafkaConsumer<String ,String > kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        while (true){
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofMillis(1000));
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            while (iterator.hasNext()){
                ConsumerRecord<String, String> next = iterator.next();
                System.out.println(next.value());
            }
        }
    }
}
