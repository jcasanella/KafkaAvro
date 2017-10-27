package com.learn.big.data.model;

import java.io.Serializable;

public class Product implements Serializable  {

    long sku;

    String score;

    String name;

    String source;

    String type;

    long productId;

    String manufacturer;

    String modelNumber;

    String image;

    public Product() {

    }

    @Override
    public String toString() {
        return "Product{" +
                "sku=" + sku +
                ", score='" + score + '\'' +
                ", name='" + name + '\'' +
                ", source='" + source + '\'' +
                ", type='" + type + '\'' +
                ", productId=" + productId +
                ", manufacturer='" + manufacturer + '\'' +
                ", modelNumber='" + modelNumber + '\'' +
                ", image='" + image + '\'' +
                '}';
    }

    public Product(long sku, String score, String name, String source, String type, long productId,
                   String manufacturer, String modelNumber, String image) {
        this.sku = sku;
        this.score = score;
        this.name = name;
        this.source = source;
        this.type = type;
        this.productId = productId;
        this.manufacturer = manufacturer;
        this.modelNumber = modelNumber;
        this.image = image;
    }

    public long getSku() {
        return sku;
    }

    public void setSku(long sku) {
        this.sku = sku;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public void setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
    }

    public String getModelNumber() {
        return modelNumber;
    }

    public void setModelNumber(String modelNumber) {
        this.modelNumber = modelNumber;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }
}