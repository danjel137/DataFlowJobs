package readFromFileAndWriteToBucket;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.util.HashMap;
import java.util.Map;

public class Computations {

    public static void main(String[] args) {
        final DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        Map<String, String> map = new HashMap<>();
        for (String currentArg : args) {
            String[] curr = currentArg.split("=");

            map.put(curr[0].substring(2), curr[1]);
        }

        options.setProject(map.get("project"));
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation(map.get("GcpTempLocation"));
        options.setRegion(map.get("region"));
        options.setJobName(map.get("jobName"));

        Pipeline pipeline=Pipeline.create(options);
        pipeline.apply("readPath",TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\datflow-jobs\\src\\main\\resources\\car.csv"))
                .apply("removeHeader", ParDo.of(new RemoveHeader()))
                .apply("getDateCreatedAndAge",ParDo.of(new MakeAgeFn()))
                .apply("outputLikeStringKV",ParDo.of(new DoFn<KV<String, Integer>, String>() {
                    @ProcessElement
                    public void apply(ProcessContext c){
                        String st=c.element().getKey()+": "+c.element().getValue();
                        c.output(st);
                    }
                }))
        .apply("writeToBucket",TextIO.write().to("danjelbucket/outputReadingFromFileLocally"));
        pipeline.run();
    }


}
