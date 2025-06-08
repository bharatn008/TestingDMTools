package com.example.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.StreamingOptions;

public interface PipelineOptions extends StreamingOptions {
    @Description("Path to the sales CSV file")
    @Validation.Required
    String getSalesFilePath();
    void setSalesFilePath(String value);

    @Description("Path to the output enriched sales file")
    @Validation.Required
    String getOutputPath();
    void setOutputPath(String value);
}
