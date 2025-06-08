package com.example.transforms;

import com.example.model.Transaction;
import org.apache.beam.sdk.transforms.DoFn;

public class FraudRuleA extends DoFn<Transaction, Transaction> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        if (c.element().amount > 10000) c.output(c.element());
    }
}