package com.learn.big.data.kafka.client;

import com.learn.big.data.model.Product;
import com.learn.big.data.utils.SomeUtils;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaAvroClient {

    private SomeUtils su;
    private Properties properties = null;
    private KafkaConsumer<String, byte[]> consumer = null;
    private Schema schema = null;
    private Injection<GenericRecord, byte[]> recordInjection = null;
    private  String topic;

    public KafkaAvroClient(String topic) {

        this.topic = topic;
        su = new SomeUtils();

        init(); // Initialize the properties
        getShema(); // Parse the schema

        consumer.subscribe(Arrays.asList(topic));   // Subscribe topic
    }

    private void init() {

        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("group.id", "bestbuyGroup");

        consumer = new KafkaConsumer<String, byte[]>(properties);
    }

    public Schema getShema() {

        try {
            Schema.Parser parser = new Schema.Parser();

            // Get schema from properties
            String avro_schema = su.getSchema();

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
        long productId =  Long.parseLong(generic.get("type").toString());
        String manufacturer = generic.get("manufacturer").toString();
        String modelNumber = generic.get("modelNumber").toString();
        String image = generic.get("image").toString();

        return new Product(sku, score, name, source, type, productId, manufacturer, modelNumber, image);
    }

}
