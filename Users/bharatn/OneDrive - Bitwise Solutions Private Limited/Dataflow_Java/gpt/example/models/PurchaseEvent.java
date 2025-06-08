package com.example.models;

public class PurchaseEvent {
    public String customerId;
    public double amount;
    public String status; // "SUCCESS" or "FAILED"
    public long timestamp;

    public PurchaseEvent() {}
    public PurchaseEvent(String customerId, double amount, String status, long timestamp) {
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.timestamp = timestamp;
    }
}
