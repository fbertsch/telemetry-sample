package com.mozilla.telemetryexample;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import java.io.Serializable;

public class Stream {
    /** A {@link CombineFn} that sums */
    public static class Aggregate extends CombineFn<String, Aggregate.Accum, String> {
        public Accum createAccumulator() { return new Accum(); }
        public class Accum implements Serializable {
            private int sum = 0;
        }
        public Accum addInput(Accum accum, String input) { accum.sum += Integer.parseInt(input); return accum; }
        public Accum mergeAccumulators(Iterable<Accum> accums) {
            Accum merged = createAccumulator();
            for (Accum accum : accums) {
                merged.sum += accum.sum;
            }
            return merged;
        }
        public String extractOutput(Accum accum) { return new Integer(accum.sum).toString(); }
    }

    /**
     * Options supported by {@link Stream}.
     *
     * <p>Inherits standard configuration options.
     */
    private interface StreamOptions extends PipelineOptions {
        @Description("Input path to read from")
        @Required
        String getInput();
        void setInput(String value);

        @Description("Output path to write to")
        @Required
        String getOutput();
        void setOutput(String value);

        @Description("Fixed window duration, in minutes")
        @Default.Long(1)
        long getWindowSize();
        void setWindowSize(long value);
    }

    /**
     * Runs streaming pipeline.
     *
     */
    public static void main(String[] args) {
        StreamOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(StreamOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply(TextIO.read().from(options.getInput()))
            .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
            .apply(Combine.globally(new Aggregate()).withoutDefaults())
            .apply(TextIO.write().to(options.getOutput()));

        PipelineResult result = pipeline.run();
    }
}
