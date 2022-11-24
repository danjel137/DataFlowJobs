package readFromFileAndWriteToBucket;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.Calendar;

public class MakeAgeFn extends DoFn<String, KV<String,Integer>> {
    @ProcessElement
    public void apply(ProcessContext c){
        String[]fields=c.element().split(",");

        String make=fields[0];
        int currentYear= Calendar.getInstance().get(Calendar.YEAR);
        int age=currentYear-Integer.parseInt(fields[7]);
        c.output(KV.of(make,age));
    }
}
