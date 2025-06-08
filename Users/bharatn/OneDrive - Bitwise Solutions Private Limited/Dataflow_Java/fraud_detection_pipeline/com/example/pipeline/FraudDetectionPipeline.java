package com.example.pipeline;

import com.example.model.Transaction;
import com.example.transforms.DeduplicateFn;
import com.google.api.services.bigquery.model.*;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import java.util.Map;

public class FraudDetectionPipeline {

    public static void main(String[] args) {
        StreamingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline p = Pipeline.create(options);
        Gson gson = new Gson();

        PCollectionView<Map<String, TableRow>> customerProfiles = p
            .apply("ReadCustomerProfiles", BigQueryIO.readTableRows().from("your-project:dataset.customer_profiles"))
            .apply("ToMap", View.<String, TableRow>asMap(row -> row.get("customerId").toString()));

        PCollection<Transaction> transactions = p
            .apply("ReadPubSub", PubsubIO.readStrings().fromTopic("projects/your-project/topics/transactions"))
            .apply("ParseJson", MapElements.into(TypeDescriptor.of(Transaction.class))
                .via(json -> gson.fromJson(json, Transaction.class)))
            .apply("Deduplicate", ParDo.of(new DeduplicateFn()));

        final TupleTag<KV<Transaction, TableRow>> suspiciousTag = new TupleTag<>(){};
        final TupleTag<KV<Transaction, TableRow>> normalTag = new TupleTag<>(){};

        PCollectionTuple tagged = transactions
            .apply("EnrichAndDetectFraud", ParDo.of(new DoFn<Transaction, KV<Transaction, TableRow>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    Map<String, TableRow> profiles = c.sideInput(customerProfiles);
                    Transaction tx = c.element();
                    TableRow profile = profiles.get(tx.customerId);
                    if (profile != null) {
                        double avg = Double.parseDouble(profile.get("avgAmount").toString());
                        if (tx.amount > 3 * avg) {
                            c.output(suspiciousTag, KV.of(tx, profile));
                        } else {
                            c.output(normalTag, KV.of(tx, profile));
                        }
                    }
                }
            }).withSideInputs(customerProfiles).withOutputTags(normalTag, TupleTagList.of(suspiciousTag)));

        tagged.get(suspiciousTag)
            .apply("ToPubSub", MapElements.into(TypeDescriptors.strings()).via(kv -> gson.toJson(kv.getKey())))
            .apply(PubsubIO.writeStrings().to("projects/your-project/topics/suspicious-transactions"));

        tagged.get(suspiciousTag)
            .apply("SuspiciousToBQ", MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(kv -> new TableRow().set("transactionId", kv.getKey().transactionId)
                                         .set("amount", kv.getKey().amount)
                                         .set("customerId", kv.getKey().customerId)))
            .apply(BigQueryIO.writeTableRows().to("your-project:fraud.suspicious")
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        tagged.get(normalTag)
            .apply("NormalToBQ", MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(kv -> new TableRow().set("transactionId", kv.getKey().transactionId)
                                         .set("amount", kv.getKey().amount)
                                         .set("customerId", kv.getKey().customerId)))
            .apply(BigQueryIO.writeTableRows().to("your-project:fraud.normal")
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run();
    }
}