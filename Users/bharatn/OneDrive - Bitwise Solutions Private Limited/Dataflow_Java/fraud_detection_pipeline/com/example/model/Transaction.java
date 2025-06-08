package com.example.model;

public class Transaction {
    public String transactionId;
    public String customerId;
    public double amount;
    public long timestamp;
    public String location;

    public Transaction() {}
    public Transaction(String transactionId, String customerId, double amount, long timestamp, String location) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.amount = amount;
        this.timestamp = timestamp;
        this.location = location;
    }
}