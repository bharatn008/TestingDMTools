package com.example.transforms;

import com.example.models.Sale;
import com.example.utils.SchemaUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class FormatTransform extends PTransform<PCollection<Sale>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<Sale> input) {
        return input.apply("Format Sales Record", ParDo.of(new DoFn<Sale, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(SchemaUtils.formatSale(c.element()));
            }
        }));
    }
}
