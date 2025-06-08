package com.example.transforms;

import com.example.models.Sale;
import com.example.utils.SchemaUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class SalesTransform extends PTransform<PCollection<String>, PCollection<Sale>> {
    @Override
    public PCollection<Sale> expand(PCollection<String> input) {
        return input.apply("Parse Sales Records", ParDo.of(new DoFn<String, Sale>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(SchemaUtils.parseSale(c.element()));
            }
        }));
    }
}
