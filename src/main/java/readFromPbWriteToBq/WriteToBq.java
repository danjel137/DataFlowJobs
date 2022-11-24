package readFromPbWriteToBq;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteToBq {

    public static void main(String[] args) {
        final DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        final Logger LOGGER = LoggerFactory.getLogger(WriteToBq.class);
        options.setProject("symmetric-hull-368913");
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation("gs://bucket_for_bqq/tempForBq");
        options.setRegion("europe-west1");
        options.setJobName("readFromPBWriteToBqWithTableROw");

        final Pipeline pipeline = Pipeline.create(options);
        PCollection<TableRow> dataFromPB = pipeline.apply("ReadFromPubSub", PubsubIO.readStrings().fromSubscription("projects/symmetric-hull-368913/subscriptions/danjelpubsub"))

                .apply("AddToArrayKVModelYear", ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void apply(ProcessContext c) {
                        String arr[] = c.element().split(",");
                        c.output(KV.of(arr[0], arr[1]));
                    }
                }))
                .apply("addToTableValue", ParDo.of(new ConvertorStringsBq()));

        dataFromPB.apply(BigQueryIO.writeTableRows().to("symmetric-hull-368913.KVmodelYear.carModelYear")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


//        pipeline.apply("readFromPb",PubsubIO.readStrings().fromTopic("projects/symmetric-hull-368913/topics/danjel_pub"))
//                        .apply("printConsole", ParDo.of(new DoFn<String, String>() {
//                            @ProcessElement
//                            public void apply (ProcessContext c){
//                                LOGGER.info(c.element());
//                            }
//                        }));

        pipeline.run();

    }
}
