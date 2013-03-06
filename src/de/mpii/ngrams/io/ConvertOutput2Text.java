package de.mpii.ngrams.io;

import de.mpii.ngrams.util.IntArrayWritable;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Converts n-gram statistics in our input-sequence format into plain text.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class ConvertOutput2Text extends Configured implements Tool {

    public static final class Map extends Mapper<IntArrayWritable, IntWritable, Text, IntWritable> {

        // dictionary mapping term identifiers to terms
        private final Int2ObjectOpenHashMap<String> dict = new Int2ObjectOpenHashMap<String>();

        // singleton output key -- for efficiency reasons
        private final Text outKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                HashMap<String, String> dictionaryFiles = new HashMap<String, String>();
                for (Path cachedPath : DistributedCache.getLocalCacheFiles(context.getConfiguration())) {
                    if (cachedPath.toString().contains("dic") && cachedPath.toString().contains("part")) {
                        String file = cachedPath.toString();
                        dictionaryFiles.put(file.substring(file.lastIndexOf("/")), file);
                    }
                }
                ArrayList<String> fileNames = new ArrayList<String>(dictionaryFiles.keySet());
                for (String fileName : fileNames) {
                    BufferedReader br = new BufferedReader(new FileReader(dictionaryFiles.get(fileName)));
                    while (br.ready()) {
                        String[] tokens = br.readLine().split("\t");
                        String term = tokens[0];
                        int tid = Integer.parseInt(tokens[1]);
                        dict.put(tid, term);
                    }
                    br.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void map(IntArrayWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            int[] contents = key.getContents();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < contents.length; i++) {
                sb.append((i > 0 ? " " : "") + dict.get(contents[i]));
            }
            outKey.set(sb.toString());
            context.write(outKey, value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Please specify <output> <input> <text_output> as parameters");
        }

        // read job parameters from commandline arguments
        String output = args[0];
        String input = args[1];
        String text = args[2];

        // delete output directory if it exists
        FileSystem.get(getConf()).delete(new Path(args[2]), true);

        Job job1 = new Job(getConf());

        // set job name and options
        job1.setJobName("Convert n-grams at " + output);
        job1.setJarByClass(this.getClass());

        // set input and output paths        
        SequenceFileInputFormat.setInputPaths(job1, new Path(output + "/part*"));
        TextOutputFormat.setOutputPath(job1, new Path(text));

        // set input and output format        
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // set mapper and reducer class
        job1.setMapperClass(Map.class);

        // set the number of reducers
        job1.setNumReduceTasks(0);

        // add dictionary to distributed cache
        for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(input + "/dic"))) {
            if (file.getPath().toString().contains("part")) {
                DistributedCache.addCacheFile(file.getPath().toUri(), job1.getConfiguration());
            }
        }

        // map output classes
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // start job
        job1.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConvertOutput2Text(), args);
        System.exit(exitCode);
    }
}
