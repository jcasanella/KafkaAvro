package com.learn.big.data.kafka.producer;

import com.learn.big.data.model.Product;
import com.learn.big.data.utils.SomeUtils;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaAvroProducer {

    private SomeUtils su;
    private Properties properties = null;
    private Injection<GenericRecord, byte[]> recordInjection = null;
    private Schema schema = null;
    private KafkaProducer<String, byte[]> producer = null;
    private String topic;

    public KafkaAvroProducer(String topic) {

        su = new SomeUtils();
        this.topic = topic;

        init(); // Init properties
        getShema(); // Get the schema
    }

    private void init() {

        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<String, byte[]>(properties);
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

    private byte[] createMessage(Product prod) {

        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("sku", prod.getSku());
        avroRecord.put("score", prod.getScore());
        avroRecord.put("name", prod.getName());
        avroRecord.put("source", prod.getSource());
        avroRecord.put("type", prod.getType());
        avroRecord.put("productId", prod.getProductId());
        avroRecord.put("manufacturer", prod.getManufacturer());
        avroRecord.put("modelNumber", prod.getModelNumber());
        avroRecord.put("image", prod.getImage());

        return recordInjection.apply(avroRecord);
    }

    public Future<RecordMetadata> send(Product prod) {

        byte[] msg = createMessage(prod);
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, msg);
        return producer.send(record);
    }

    KafkaProducer<String, byte[]> getProducer() {

        return this.producer;
    }

    public void close() {

        producer.flush();
        producer.close();
    }
}
