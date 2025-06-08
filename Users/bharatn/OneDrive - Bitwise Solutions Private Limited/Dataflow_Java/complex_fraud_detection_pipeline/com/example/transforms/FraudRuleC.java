package com.example.transforms;

import com.example.model.Transaction;
import org.apache.beam.sdk.transforms.DoFn;

public class FraudRuleC extends DoFn<Transaction, Transaction> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        if (c.element().merchantId.equals("fraud123")) c.output(c.element());
    }
}