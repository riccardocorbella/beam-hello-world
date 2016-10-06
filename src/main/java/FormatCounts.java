import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Created by rcorbella on 05/10/16.
 */
public class FormatCounts extends DoFn<KV<String, Integer>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        KV<String, Integer> wordCount = c.element();
        c.output("(" + wordCount.getKey() + "," + wordCount.getValue() + ")");
    }
}
