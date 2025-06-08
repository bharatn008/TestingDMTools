package com.example.models;

public class Sale {
    private String saleId;
    private String productId;
    private String storeId;
    private int quantity;
    private double totalAmount;

    public Sale() {}

    public Sale(String saleId, String productId, String storeId, int quantity, double totalAmount) {
        this.saleId = saleId;
        this.productId = productId;
        this.storeId = storeId;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
    }

    public String getSaleId() { return saleId; }
    public void setSaleId(String saleId) { this.saleId = saleId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public String getStoreId() { return storeId; }
    public void setStoreId(String storeId) { this.storeId = storeId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }
}
