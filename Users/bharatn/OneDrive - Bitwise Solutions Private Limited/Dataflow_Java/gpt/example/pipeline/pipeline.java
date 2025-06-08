package com.example.pipeline;

import com.example.models.PurchaseEvent;
import com.example.transforms.AddCustomerTierFn;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.joda.time.Duration;

public class EcommercePipeline {

    public static void main(String[] args) {
        StreamingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingOptions.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);
        Gson gson = new Gson();

        pipeline
            .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic("projects/your-project/topics/purchase-events"))

            // Parse JSON into PurchaseEvent
            .apply("ParseJson", MapElements.into(TypeDescriptor.of(PurchaseEvent.class))
                .via((String json) -> gson.fromJson(json, PurchaseEvent.class)))

            // Filter only successful purchases
            .apply("FilterSuccess", Filter.by((PurchaseEvent e) -> "SUCCESS".equals(e.status)))

            // Enrich with customer tier
            .apply("AddCustomerTier", ParDo.of(new AddCustomerTierFn()))

            // Window into 1-minute intervals
            .apply("Window1Min", Window.<KV<String, Double>>into(FixedWindows.of(Duration.standardMinutes(1))))

            // Sum purchase amount per tier
            .apply("SumPerTier", Sum.doublesPerKey())

            // Convert to TableRow for BigQuery
            .apply("ToTableRow", MapElements.into(TypeDescriptor.of(com.google.api.services.bigquery.model.TableRow.class))
                .via((KV<String, Double> kv) -> new com.google.api.services.bigquery.model.TableRow()
                    .set("tier", kv.getKey())
                    .set("total_sales", kv.getValue())
                    .set("window_end", System.currentTimeMillis())))

            // Write to BigQuery
            .apply("WriteToBQ", BigQueryIO.writeTableRows()
                .to("your-project:analytics.sales_by_tier")
                .withSchema(new com.google.api.services.bigquery.model.TableSchema().setFields(java.util.List.of(
                    new com.google.api.services.bigquery.model.TableFieldSchema().setName("tier").setType("STRING"),
                    new com.google.api.services.bigquery.model.TableFieldSchema().setName("total_sales").setType("FLOAT"),
                    new com.google.api.services.bigquery.model.TableFieldSchema().setName("window_end").setType("TIMESTAMP")
                )))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pipeline.run();
    }
}
