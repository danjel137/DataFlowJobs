package readFromPbWriteToBq;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;


public class ConvertorStringsBq extends DoFn<KV<String,String>, TableRow> {
    @ProcessElement
    public void processing(ProcessContext c){
        TableRow tbrw=new TableRow().set("model",c.element().getKey().toString()).set("yearCreated",c.element().getValue());
        c.output(tbrw);
    }
}
