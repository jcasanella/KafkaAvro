package com.learn.big.data.kafka.client;

import com.learn.big.data.model.Product;
import com.learn.big.data.utils.ReadResources;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaAvroClient {

    private Properties properties = null;
    private KafkaConsumer<String, byte[]> consumer = null;
    private Schema schema = null;
    private Injection<GenericRecord, byte[]> recordInjection = null;
    private  String topic;

    public KafkaAvroClient(String topic) {

        this.topic = topic;

        init(); // Initialize the properties
        getShema(); // Parse the schema
    }

    private void init() {

        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bestbuy");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Read from the beginning
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, byte[]>(properties);
        consumer.subscribe(Arrays.asList(topic));   // Subscribe topic
    }

    public Schema getShema() {

        try {
            Schema.Parser parser = new Schema.Parser();

            // Get schema from properties
            String avro_schema = ReadResources.getSchema();

            schema = parser.parse(avro_schema);
            recordInjection = GenericAvroCodecs.toBinary(schema);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return schema;
    }

    public ConsumerRecords<String, byte[]> poll(int time) {

        return consumer.poll(100);
    }

    public Product getProduct(ConsumerRecord<String, byte[]> record) {

        GenericRecord generic = recordInjection.invert(record.value()).get();

        long sku = Long.parseLong(generic.get("sku").toString());
        String score = generic.get("score").toString();
        String name  = generic.get("name").toString();
        String source = generic.get("source").toString();
        String type =  generic.get("type").toString();
        long productId =  Long.parseLong(generic.get("productId").toString());
        String manufacturer = generic.get("manufacturer").toString();
        String modelNumber = generic.get("modelNumber").toString();
        String image = generic.get("image").toString();

        return new Product(sku, score, name, source, type, productId, manufacturer, modelNumber, image);
    }

}
