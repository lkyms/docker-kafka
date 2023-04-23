package course.kafka.example.avro;

import course.kafka.example.avro.v1.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Arrays;
import java.util.Properties;

public class AvroProducer {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "course.kafka.example.avro.AvroSerializer");

        User user = User.newBuilder().setFavoriteNumber(1).setUserId(10001l).setName("jeff").setFavoriteColor("red").build();
        ProductOrder order = ProductOrder.newBuilder().setOrderId(2000l).setUserId(user.getUserId()).setProductId(101l).build();



        Producer<String, Object> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            Iterable<Header> headers = Arrays.asList(new RecordHeader("schema", user.getClass().getName().getBytes()));;
            producer.send(new ProducerRecord<String, Object>("my-topic",null, ""+user.getUserId(), user,headers));
        }
        for (int i = 0; i < 100; i++) {
            Iterable<Header> headers = Arrays.asList(new RecordHeader("schema", order.getClass().getName().getBytes()));;
            producer.send(new ProducerRecord<String, Object>("my-topic",null, ""+order.getUserId(), order,headers));
        }

        System.out.println("send successful");
        producer.close();

    }
}
