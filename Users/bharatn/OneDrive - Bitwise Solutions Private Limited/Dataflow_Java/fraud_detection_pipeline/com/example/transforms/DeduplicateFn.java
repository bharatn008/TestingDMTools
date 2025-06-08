package com.example.transforms;

import com.example.model.Transaction;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;

public class DeduplicateFn extends DoFn<Transaction, Transaction> {
    @StateId("seenIds")
    private final StateSpec<ValueState<Boolean>> seenIds = StateSpecs.value();

    @TimerId("expiry")
    private final TimerSpec expiryTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(ProcessContext c, @StateId("seenIds") ValueState<Boolean> seen,
                               @TimerId("expiry") Timer timer) {
        if (seen.read() != null) return;
        seen.write(true);
        timer.offset(Duration.standardMinutes(10)).setRelative();
        c.output(c.element());
    }

    @OnTimer("expiry")
    public void onExpiry(@StateId("seenIds") ValueState<Boolean> seen) {
        seen.clear();
    }
}