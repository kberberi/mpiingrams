package de.mpii.ngrams.methods;

import de.mpii.ngrams.util.IntArrayWritable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mapdb.DBMaker;

/**
 * Apriori-style computation of (all|closed|maximal) n-gram frequencies.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class NGAprioriScan extends Configured implements Tool {

    public static final class MapOne extends Mapper<LongWritable, IntArrayWritable, IntArrayWritable, IntWritable> {

        // minimum support threshold
        private int minsup;

        // currently considered n-gram length
        private int k;

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable(1);

        // in-memory dictionary of frequent (k-1)-grams
        private final Set<IntArrayWritable> dict = DBMaker.newTempHashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 1);
            k = context.getConfiguration().getInt("de.mpii.ngrams.k", 1);
            try {
                if (k <= 2) {
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
                            int support = Integer.parseInt(tokens[2]);
                            if (support >= minsup) {
                                int tid = Integer.parseInt(tokens[1]);
                                dict.add(new IntArrayWritable(new int[]{tid}));
                            }
                        }
                        br.close();
                    }
                } else {
                    for (Path cachedPath : DistributedCache.getLocalCacheFiles(context.getConfiguration())) {
                        if (cachedPath.toString().contains(Integer.toString(k - 1)) && cachedPath.toString().contains("part")) {
                            SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(context.getConfiguration()), cachedPath, context.getConfiguration());
                            IntArrayWritable key = new IntArrayWritable();
                            while (reader.next(key)) {
                                dict.add(new IntArrayWritable(key.getContents()));
                            }
                            reader.close();
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void map(LongWritable key, IntArrayWritable value, Context context) throws IOException, InterruptedException {
            int[] contents = value.getContents();
            for (int b = 0; b < contents.length - k + 1; b++) {

                if (k == 1) {
                    outKey.setContents(contents, b, b + 1);
                    if (!dict.contains(outKey)) {
                        continue;
                    }
                } else {

                    // left (m-1)-gram
                    outKey.setContents(contents, b, b + k - 1);
                    if (!dict.contains(outKey)) {
                        continue;
                    }

                    // right (m-1)-gram
                    outKey.setContents(contents, b + 1, b + k);
                    if (!dict.contains(outKey)) {
                        continue;
                    }
                }

                // output m-gram
                outKey.setContents(contents, b, b + k);
                context.write(outKey, outValue);
            }
        }
    }

    public static final class CombineOne extends Reducer<IntArrayWritable, IntWritable, IntArrayWritable, IntWritable> {

        private final IntWritable outValue = new IntWritable();

        @Override
        protected void reduce(IntArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int support = 0;
            for (IntWritable value : values) {
                support += value.get();
            }
            outValue.set(support);
            context.write(key, outValue);
        }
    }

    public static final class ReduceOne extends Reducer<IntArrayWritable, IntWritable, IntArrayWritable, IntWritable> {

        // minimum support threshold
        private int minsup;

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 1);
        }

        @Override
        protected void reduce(IntArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int support = 0;
            for (IntWritable value : values) {
                support += value.get();
            }
            if (support >= minsup) {
                outValue.set(support);
                context.write(key, outValue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        if (args.length < 6) {
            System.err.println("Please specify <input> <output> <minimum_support> <maximum_length> <type> <number_of_reducers> as parameters");
        }

        // read job parameters from commandline arguments
        String input = args[0];
        String output = args[1];
        int minsup = Integer.parseInt(args[2]);
        int maxlen = Integer.parseInt(args[3]);
        maxlen = (maxlen == 0 ? Integer.MAX_VALUE : maxlen);

        // parse optional parameters        
        int type = Integer.parseInt(args[4]);
        int numred = Integer.parseInt(args[5]);

        // delete output directory if it exists
        FileSystem.get(getConf()).delete(new Path(args[1]), true);

        int k = 1;
        boolean done = false;
        while (!done) {
            Job job1 = new Job(getConf());

            // set job name and options
            job1.setJobName("NG-AprioriScan (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 1, Round " + k + ")");
            job1.setJarByClass(this.getClass());
            job1.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
            job1.getConfiguration().setInt("de.mpii.ngrams.maxlen", maxlen);
            job1.getConfiguration().setInt("de.mpii.ngrams.type", type);
            job1.getConfiguration().setInt("de.mpii.ngrams.k", k);

            // set input and output paths        
            SequenceFileInputFormat.setInputPaths(job1, new Path(input + "/seq"));
            SequenceFileOutputFormat.setOutputPath(job1, new Path(output + (type == NG.ALL ? "/result/" + k : "/tmp/" + k)));

            // set input and output format        
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);

            // set mapper and reducer class
            job1.setMapperClass(MapOne.class);
            job1.setReducerClass(ReduceOne.class);

            // set the number of reducers
            job1.setNumReduceTasks(numred);

            // add dictionary or output from previous round to distributed cache
            if (k <= 2) {
                for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(input + "/dic"))) {
                    if (file.getPath().toString().contains("part")) {
                        DistributedCache.addCacheFile(file.getPath().toUri(), job1.getConfiguration());
                    }
                }
            } else {
                for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(output + (type == NG.ALL ? "/result/" + (k - 1) : "/tmp/" + (k - 1))))) {
                    if (file.getPath().toString().contains("part")) {
                        DistributedCache.addCacheFile(file.getPath().toUri(), job1.getConfiguration());
                    }
                }
            }

            // map output classes
            job1.setMapOutputKeyClass(IntArrayWritable.class);
            job1.setMapOutputValueClass(IntWritable.class);
            job1.setOutputKeyClass(IntArrayWritable.class);
            job1.setOutputValueClass(IntWritable.class);
            job1.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerComplete.class);
            job1.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

            // start job
            job1.waitForCompletion(true);

            // check whether any records were output
            done = (job1.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue() == 0 || k == maxlen);

            k++;
        }

        if (type != NG.ALL) {

            // forward
            Job job2 = new Job(getConf());

            // set job name and options
            job2.setJobName("NG-AprioriScan (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 2)");
            job2.setJarByClass(this.getClass());
            job2.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
            job2.getConfiguration().setInt("de.mpii.ngrams.type", type);

            // set input and output paths        
            SequenceFileInputFormat.setInputPaths(job2, new Path(output + "/tmp/*/part*"));
            SequenceFileOutputFormat.setOutputPath(job2, new Path(output + "/forward"));

            // set input and output format        
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);

            // set mapper and reducer class
            job2.setMapperClass(NGClosedMaximal.MapForward.class);
            job2.setReducerClass(NGClosedMaximal.ReduceForward.class);

            // set the number of reducers
            job2.setNumReduceTasks(numred);

            // map output classes
            job2.setMapOutputKeyClass(IntArrayWritable.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(IntArrayWritable.class);
            job2.setOutputValueClass(IntWritable.class);
            job2.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerFirstOnly.class);
            job2.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

            // start job
            job2.waitForCompletion(true);

            // backward
            Job job3 = new Job(getConf());

            // set job name and options
            job3.setJobName("NG-AprioriScan (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 3)");
            job3.setJarByClass(this.getClass());
            job3.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
            job3.getConfiguration().setInt("de.mpii.ngrams.type", type);

            // set input and output paths        
            SequenceFileInputFormat.setInputPaths(job3, new Path(output + "/forward"));
            SequenceFileOutputFormat.setOutputPath(job3, new Path(output + "/result"));

            // set input and output format        
            job3.setInputFormatClass(SequenceFileInputFormat.class);
            job3.setOutputFormatClass(SequenceFileOutputFormat.class);

            // set mapper and reducer class
            job3.setMapperClass(NGClosedMaximal.MapReverse.class);
            job3.setReducerClass(NGClosedMaximal.ReduceReverse.class);

            // set the number of reducers
            job3.setNumReduceTasks(numred);

            // map output classes
            job3.setMapOutputKeyClass(IntArrayWritable.class);
            job3.setMapOutputValueClass(IntWritable.class);
            job3.setOutputKeyClass(IntArrayWritable.class);
            job3.setOutputValueClass(IntWritable.class);
            job3.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerFirstOnly.class);
            job3.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

            // start job
            job3.waitForCompletion(true);

            // delete temporary directory
            FileSystem.get(getConf()).delete(new Path(output + "/tmp"), true);
            FileSystem.get(getConf()).delete(new Path(output + "/forward"), true);
        }

        System.err.println("Took " + (System.currentTimeMillis() - start) + " ms");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NGAprioriScan(), args);
        System.exit(exitCode);
    }
}
