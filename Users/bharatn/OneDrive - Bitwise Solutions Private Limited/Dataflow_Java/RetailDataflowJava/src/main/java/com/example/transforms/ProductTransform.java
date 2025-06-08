package com.example.transforms;

import com.example.models.Sale;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class ProductTransform extends PTransform<PCollection<Sale>, PCollection<Sale>> {
    @Override
    public PCollection<Sale> expand(PCollection<Sale> input) {
        return input.apply("Enrich Sales with Product Info", ParDo.of(new DoFn<Sale, Sale>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Sale sale = c.element();
                sale.setStoreId(sale.getStoreId() + "_enriched");
                c.output(sale);
            }
        }));
    }
}
