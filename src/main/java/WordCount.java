import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by rcorbella on 05/10/16.
 */
public class WordCount {
    public static void main(String[] args) {

        String inputFileName = null;
        String outputFileName = null;

        if (args.length < 2) {
            System.out.println("missing arguments:\n\tinput_file\n\toutput_prefix");
        } else {
            inputFileName = args[0];
            outputFileName = args[1];
        }

        // Start by defining the options for the pipeline.
        PipelineOptions options = PipelineOptionsFactory.create();
        // Set the runner.
        options.setRunner(DirectRunner.class);
        // Then create the pipeline (a pipeline is a dag). The runner is determined by the options.
        Pipeline p = Pipeline.create(options);
        // Add to the pipeline the root transformation.
        PCollection<String> lines = p.apply(TextIO.Read.from(inputFileName));
        // Add to the pipeline two transformation.
        // The first one split every line into words and for each word emit a couple (word, 1).
        // The second one count the occurrences of each word.
        PCollection<KV<String, Integer>> wordCounts = lines.apply(ParDo.of(new LineToWordCounts()))
                .apply(Combine.<String, Integer, Integer>perKey(new Sum.SumIntegerFn()));
        // Add to the pipeline a transformation that convert the elements of the input PCollection into strings.
        PCollection<String> formattedWordCounts = wordCounts.apply(ParDo.of(new FormatCounts()));
        // Add to the pipeline the last transformation that save on the specified output file the input PCollection.
        formattedWordCounts.apply(TextIO.Write.to(outputFileName));
        // Run the pipeline.
        p.run();
    }
}
