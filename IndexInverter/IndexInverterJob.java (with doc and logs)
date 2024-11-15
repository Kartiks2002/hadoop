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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * IndexInverterJob is a MapReduce job that inverts an index.
 * The input file contains records where the first value is an ID and 
 * subsequent values are associated attributes. The job outputs an inverted index,
 * with each attribute as a key and the associated IDs as the value.
 */
public class IndexInverterJob extends Configured implements Tool {

    // Logger for logging the job execution details
    private static final Log LOG = LogFactory.getLog(IndexInverterJob.class);

    /**
     * Mapper class for the IndexInverterJob.
     * It takes a CSV line as input, extracts the first word as the ID,
     * and for each subsequent word, it emits the word as the key and the ID as the value.
     */
    public static class IndexInverterMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        /**
         * The map function processes each line from the input file.
         * It splits the line into words, and for each word after the first,
         * it emits the word as the key and the first word (ID) as the value.
         * 
         * @param key    The byte offset of the current line in the input file.
         * @param value  The text of the current line.
         * @param context  The context object for emitting output key-value pairs.
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split(",");
            
            if (words.length < 2) {
                LOG.warn("Skipping line due to insufficient values: " + value.toString());
                return; // skip the line if there are fewer than 2 words
            }

            outputValue.set(words[0]);  // Set the ID as the value
            
            for (int i = 1; i < words.length; i++) {
                outputKey.set(words[i]);  // Set the attribute as the key
                context.write(outputKey, outputValue);  // Emit (attribute, ID)
                LOG.info("Mapped: " + outputKey.toString() + " -> " + outputValue.toString());
            }
        }
    }

    /**
     * Reducer class for the IndexInverterJob.
     * It receives a list of IDs for each attribute and concatenates them into a comma-separated string.
     */
    public static class IndexInverterReducer extends
            Reducer<Text, Text, Text, Text> {

        private Text outputValue = new Text();

        /**
         * The reduce function concatenates the IDs for each key (attribute)
         * and outputs the attribute and the comma-separated list of IDs.
         * 
         * @param key    The attribute.
         * @param values The list of IDs associated with the attribute.
         * @param context The context object for emitting the output key-value pairs.
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for (Text value : values) {
                builder.append(value.toString()).append(",");
            }
            
            // Remove the last comma
            if (builder.length() > 0) {
                builder.deleteCharAt(builder.length() - 1);
            }

            outputValue.set(builder.toString());
            context.write(key, outputValue);  // Emit (attribute, list of IDs)
            LOG.info("Reduced: " + key.toString() + " -> " + outputValue.toString());
        }
    }

    /**
     * Configures and runs the MapReduce job.
     * 
     * @param args  Command-line arguments.
     * @return 0 if the job completes successfully, 1 otherwise.
     * @throws Exception If an error occurs during the job execution.
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "IndexInverterJob");
        job.setJarByClass(IndexInverterJob.class);

        // Parse input and output paths from command line arguments
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        // Log the input and output paths
        LOG.info("Input path: " + in.toString());
        LOG.info("Output path: " + out.toString());

        // Delete the output directory if it exists
        out.getFileSystem(conf).delete(out, true);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // Set Mapper and Reducer classes
        job.setMapperClass(IndexInverterMapper.class);
        job.setReducerClass(IndexInverterReducer.class);

        // Set input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set output key and value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        LOG.info("Job configuration completed. Starting job...");

        // Wait for job completion and return success/failure status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * The main entry point for the program.
     * It runs the IndexInverterJob using ToolRunner.
     * 
     * @param args  Command-line arguments: input path and output path.
     */
    public static void main(String[] args) {
        int result;
        try {
            result = ToolRunner.run(new Configuration(),
                    new IndexInverterJob(), args);
            System.exit(result);
        } catch (Exception e) {
            LOG.error("Error running job", e);
            e.printStackTrace();
        }
    }
}
