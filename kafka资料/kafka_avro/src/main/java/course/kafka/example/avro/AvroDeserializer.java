package course.kafka.example.avro;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class AvroDeserializer implements ExtendedDeserializer {

    public static final StringDeserializer Default = new StringDeserializer();
    private static final Map DECODERS = new HashMap<>();

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        if(topic.equals("my-topic")){
            try {
               return User.getDecoder().decode(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return Default.deserialize(topic,bytes);
    }

    @Override
    public void close() {

    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        String className = null;
        for (Header header : headers) {
            if(header.key().equals("schema")){
                className = new String(header.value());
            }
        }

        if(className != null){
            try {
                BinaryMessageDecoder decoder = (BinaryMessageDecoder) DECODERS.get(className);
                if(decoder == null){
                    Class cls = Class.forName(className);
                    Method method = cls.getDeclaredMethod("getDecoder");
                    decoder= (BinaryMessageDecoder) method.invoke(cls);
                    DECODERS.put(className,decoder);
                }
                return decoder.decode(bytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return this.deserialize(topic,bytes);
    }
}
