package course.kafka.example.avro;

import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class AvroSerializer implements ExtendedSerializer {

    public static final StringSerializer Default = new StringSerializer();
    private static final Map ENCODERS = new HashMap();

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
        public byte[] serialize(String topic, Object o) {
            if(topic.equals("my-topic")){
                try {
                    return  User.getEncoder().encode((User) o).array();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            return Default.serialize(topic,  o.toString());
        }

    @Override
    public void close() {

    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object o) {

        if(o == null){
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
                BinaryMessageEncoder encoder = (BinaryMessageEncoder) ENCODERS.get(className);
                if(encoder == null){
                    Class cls = Class.forName(className);
                    Method method = cls.getDeclaredMethod("getEncoder");
                    encoder= (BinaryMessageEncoder) method.invoke(cls);
                    ENCODERS.put(className,encoder);
                }
                return encoder.encode(o).array();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return this.serialize(topic,o);
    }
}
