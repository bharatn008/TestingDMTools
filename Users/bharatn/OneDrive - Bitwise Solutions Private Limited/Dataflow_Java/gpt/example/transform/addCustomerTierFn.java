package com.example.transforms;

import com.example.models.PurchaseEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class AddCustomerTierFn extends DoFn<PurchaseEvent, KV<String, Double>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        PurchaseEvent event = c.element();
        String tier;

        // Dummy enrichment logic
        if (event.amount > 500) tier = "GOLD";
        else if (event.amount > 100) tier = "SILVER";
        else tier = "BRONZE";

        c.output(KV.of(tier, event.amount));
    }
}
