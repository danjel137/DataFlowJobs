package readFromBucketWriteToPb;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.HashMap;
import java.util.Map;

public class WriteToPb {
    public static void main(String[] args) {
        final DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        options.setProject("symmetric-hull-368913");
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation("gs://bucket_for_bqq/tempForPubsub");
        options.setRegion("europe-west1");
        options.setJobName("readFromBucketAndWriteToPb");

        final Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("readPath", TextIO.read().from("gs://danjelbucket/output-00000-of-00001"))
                .apply("WriteToPubSub", PubsubIO.writeStrings().to(
                        "projects/symmetric-hull-368913/topics/danjelpubsub"));

                pipeline.run();

    }
}
