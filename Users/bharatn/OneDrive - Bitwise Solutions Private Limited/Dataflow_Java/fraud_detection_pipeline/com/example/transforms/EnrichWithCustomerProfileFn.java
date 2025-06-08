package com.example.transforms;

import com.example.model.Transaction;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import java.util.Map;

public class EnrichWithCustomerProfileFn extends DoFn<Transaction, KV<Transaction, TableRow>> {
    private final Map<String, TableRow> profileMap;

    public EnrichWithCustomerProfileFn(Map<String, TableRow> profileMap) {
        this.profileMap = profileMap;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Transaction tx = c.element();
        TableRow profile = profileMap.get(tx.customerId);
        if (profile != null) {
            c.output(KV.of(tx, profile));
        }
    }
}