package readFromFileAndWriteToBucket;

import org.apache.beam.sdk.transforms.DoFn;

public class RemoveHeader extends DoFn<String,String> {
    @ProcessElement
    public void apply(ProcessContext c){
        String st=c.element();
        if((!st.equals("car,price,body,mileage,engV,engType,registration,year,model,drive")&& (!st.isEmpty()))){
            c.output(st);
        }
    }
}
