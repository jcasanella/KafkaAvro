package com.learn.big.data;

import com.learn.big.data.kafka.client.KafkaAvroClient;
import com.learn.big.data.model.Product;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Consumer {

    public static void main(String[] args) {

        // Set up Kafka consumer
        KafkaAvroClient client = new KafkaAvroClient("\"best_buy\"");

        while(true) {

            ConsumerRecords<String, byte[]> records = client.poll(100);
            for(ConsumerRecord<String, byte[]> record : records) {

                Product prod = client.getProduct(record);

                System.out.printf("offset: %d key: %s \n", record.offset(), record.key());
                System.out.println(prod.toString());
            }
        }

    }
}
