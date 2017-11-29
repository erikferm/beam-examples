package beam.examples.erikferm.cogroupbykey.ParseFns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ParseVideoTitles extends DoFn<String,KV<String,String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        final String[] row = c.element().split(",");
        if (row[0].contains("VideoId")){
            return;
        }
        c.output(KV.of(row[0].trim(),row[1].trim()));
    }
}
