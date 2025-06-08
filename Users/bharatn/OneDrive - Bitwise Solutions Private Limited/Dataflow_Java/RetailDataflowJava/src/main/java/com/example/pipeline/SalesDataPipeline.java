package com.example.pipeline;

import com.example.transforms.ProductTransform;
import com.example.transforms.SalesTransform;
import com.example.transforms.FormatTransform;
import com.example.models.Sale;
import com.example.options.PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class SalesDataPipeline {
    public static void main(String[] args) {
        PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> rawSales = pipeline.apply("Read Sales Data", TextIO.read().from(options.getSalesFilePath()));

        PCollection<Sale> parsedSales = rawSales.apply("Transform Sales Data", new SalesTransform());

        PCollection<Sale> enrichedSales = parsedSales.apply("Enrich with Product Data", new ProductTransform());

        PCollection<String> outputSales = enrichedSales.apply("Format Output", new FormatTransform());

        outputSales.apply("Write Output", TextIO.write().to(options.getOutputPath()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}
