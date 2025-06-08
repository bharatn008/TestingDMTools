package com.example.transforms;

import com.example.model.Transaction;
import org.apache.beam.sdk.transforms.DoFn;

public class LocationValidator extends DoFn<Transaction, Transaction> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        Transaction tx = c.element();
        if (tx.location != null && !tx.location.isEmpty()) c.output(tx);
    }
}