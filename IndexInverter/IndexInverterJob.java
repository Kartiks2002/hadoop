import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * IndexInverterJob is a Hadoop MapReduce program that reads an input file where each line contains
 * an identifier followed by words. It inverts the index by outputting each word as a key and all 
 * corresponding identifiers as values.
 */
public class IndexInverterJob extends Configured implements Tool {

    // Initialize the logger
    private static final Logger logger = LogManager.getLogger(IndexInverterJob.class);

    /**
     * The Mapper class processes each line of the input file. It splits the line by commas and outputs
     * the word (starting from the second word in the line) as the key and the first word (identifier) as the value.
     */
    public static class IndexInverterMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        /**
         * The map method processes each input line and splits it by commas. The first token is considered
         * as the identifier, and each subsequent token (word) is emitted as a key with the identifier as the value.
         * 
         * @param key the byte offset of the current line in the input file (ignored in this case)
         * @param value the input line (as Text) to process
         * @param context the Context object to write the output key-value pairs
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("Processing line: " + value.toString());

            String[] words = value.toString().split(",");
            outputValue.set(words[0]); // First word is the identifier
            for (int i = 1; i < words.length; i++) {
                outputKey.set(words[i]); // Each word is the output key
                context.write(outputKey, outputValue); // Write word as key, identifier as value
                logger.debug("Emitting key-value pair: (" + words[i] + ", " + words[0] + ")");
            }
        }
    }

    /**
     * The Reducer class collects all values (identifiers) associated with each word and concatenates them
     * into a comma-separated list.
     */
    public static class IndexInverterReducer extends Reducer<Text, Text, Text, Text> {

        private Text outputValue = new Text();

        /**
         * The reduce method takes a word as the key and all corresponding identifiers as values.
         * It concatenates all identifiers into a single comma-separated string and writes the word 
         * and its list of identifiers as the output.
         * 
         * @param key the word being processed
         * @param values an Iterable of Text containing all identifiers associated with the word
         * @param context the Context object to write the final key-value pairs (word, identifier list)
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            logger.info("Reducing key: " + key.toString());

            StringBuilder builder = new StringBuilder();
            for (Text value : values) {
                builder.append(value.toString()).append(",");
            }
            if (builder.length() > 0) {
                builder.deleteCharAt(builder.length() - 1); // Remove trailing comma
            }
            outputValue.set(builder.toString());
            context.write(key, outputValue); // Write word and concatenated identifiers
            logger.debug("Reduced value for key " + key.toString() + ": " + builder.toString());
        }
    }

    /**
     * Configures and runs the MapReduce job.
     * 
     * @param args the input and output paths for the job
     * @return 0 if the job completes successfully, 1 otherwise
     * @throws Exception if the job fails
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        logger.info("Starting IndexInverterJob");

        Job job = Job.getInstance(conf, "IndexInverterJob");
        job.setJarByClass(IndexInverterJob.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        // Log input and output paths
        logger.info("Input Path: " + in.toString());
        logger.info("Output Path: " + out.toString());

        // Delete output directory if it exists
        if (out.getFileSystem(conf).exists(out)) {
            logger.warn("Output path exists. Deleting: " + out.toString());
            out.getFileSystem(conf).delete(out, true);
        }

        // Set input and output paths
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // Set Mapper and Reducer classes
        job.setMapperClass(IndexInverterMapper.class);
        job.setReducerClass(IndexInverterReducer.class);

        // Set input and output format classes
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set output key and value types for Mapper and Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Wait for the job to complete and return the result
        logger.info("Waiting for job completion...");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * The main method, which sets up and starts the MapReduce job using ToolRunner.
     * 
     * @param args the input and output paths
     */
    public static void main(String[] args) {
        int result;
        try {
            logger.info("Running IndexInverterJob with arguments: " + String.join(", ", args));
            result = ToolRunner.run(new Configuration(), new IndexInverterJob(), args);
            logger.info("Job completed with result code: " + result);
            System.exit(result);
        } catch (Exception e) {
            logger.error("Exception occurred during job execution", e);
            System.exit(1);
        }
    }
}
