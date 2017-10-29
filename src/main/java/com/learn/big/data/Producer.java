package com.learn.big.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.learn.big.data.kafka.producer.KafkaAvroProducer;
import com.learn.big.data.net.RestClient;
import com.learn.big.data.model.Product;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.concurrent.Future;

public class Producer {

    private final static String API_KEY = "rez8vbyvftcqzn3h5hkxvm77";

    public static void main(String[] args) {

        // Start Kafka
        KafkaAvroProducer producer = new KafkaAvroProducer("best_buy");

        // Start Rest
        int currentPage = 1;
        int cont = 0;
        while(cont < 5) {

            System.out.println(RestClient.getMessage(currentPage));

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = null;

            try {
                root = mapper.readTree(RestClient.getMessage(currentPage));

                if (root.get("products").isArray()) {

                    for (final JsonNode element : root.get("products")) {

                        long sku = element.get("sku").longValue();
                        String score = element.get("score").toString();
                        String name = element.get("name").toString();
                        String source = element.get("source").toString();
                        String type = element.get("type").toString();
                        long productId = element.get("productId").longValue();
                        String manufacturer = element.get("manufacturer").toString();
                        String modelNumber = element.get("modelNumber").toString();
                        String image = element.get("image").toString();

                        Product prod = new Product(sku, score, name, source, type, productId,
                                manufacturer, modelNumber, image);

                        // Set up message
                        Future<RecordMetadata> metadata = producer.send(prod);
                        System.out.printf("Metadata offset: %d, topic: %s, partition: %d \n",
                                metadata.get().offset(), metadata.get().topic(), metadata.get().partition());

                        // Message to send
                        System.out.println(prod.toString());

                        Thread.sleep(2000);
                    }
                }

                currentPage++;
                cont++;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Flush and close the producer
        producer.close();

    }
}
