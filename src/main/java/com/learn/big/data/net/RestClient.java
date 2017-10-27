package com.learn.big.data.net;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class RestClient {

    private final static String API_KEY = "rez8vbyvftcqzn3h5hkxvm77";

    public static String getMessage(int currentPage) {

        String URL = "https://api.bestbuy.com/v1/products" +
                "((categoryPath.id=pcmcat209400050001))?" +
                "apiKey=" + API_KEY +
                "&pageSize=5"
                + "&page=" + currentPage +
                "&format=json";

        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> response = restTemplate.getForEntity(URL, String.class);

        return response.getBody();
    }
}
