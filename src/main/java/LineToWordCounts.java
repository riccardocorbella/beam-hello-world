import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Created by rcorbella on 05/10/16.
 */
public class LineToWordCounts extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String line = c.element();
        for (String word : line.split("[^a-zA-Z']+")) {
            c.output(KV.of(word, 1));
        }
    }
}
