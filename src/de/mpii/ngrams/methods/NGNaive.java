package de.mpii.ngrams.methods;

import de.mpii.ngrams.util.IntArrayWritable;
import de.mpii.ngrams.util.StringUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Naive algorithm to compute (all|closed|maximal) n-gram frequencies.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class NGNaive extends Configured implements Tool {

    public static final class MapOne extends Mapper<LongWritable, IntArrayWritable, IntArrayWritable, IntWritable> {

        // minimum support threshold
        private int minsup;

        // maximum n-gram length considered
        private int maxlen;

        // set of frequent terms
        private final IntOpenHashSet tidSet = new IntOpenHashSet();

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 1);
            maxlen = context.getConfiguration().getInt("de.mpii.ngrams.maxlen", Integer.MAX_VALUE);
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
                        int support = Integer.parseInt(tokens[2]);
                        if (support >= minsup) {
                            int tid = Integer.parseInt(tokens[1]);
                            tidSet.add(tid);
                        }
                    }
                    br.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void map(LongWritable key, IntArrayWritable value, Context context) throws IOException, InterruptedException {
            int contents[] = value.getContents();
            for (int b = 0, e = 0; b < contents.length; b++) {

                // ignore suffixes starting with infrequent term
                if (contents[b] == 0 || !tidSet.contains(contents[b])) {
                    continue;
                }

                // determine segment end
                while (e <= b || (e < contents.length && tidSet.contains(contents[e]))) {
                    e++;
                }

                // extract all subsequences
                for (int pb = b; pb < e; pb++) {
                    for (int pe = pb + 1; pe <= e && pe <= (maxlen == Integer.MAX_VALUE ? e : pb + maxlen); pe++) {
                        outKey.setContents(contents, pb, pe);
                        outValue.set(1);
                        context.write(outKey, outValue);
                    }
                }

                // adjust begin
                b = e;
            }
        }
    }

    public static final class CombineOne extends Reducer<IntArrayWritable, IntWritable, IntArrayWritable, IntWritable> {

        // singleton output value -- for efficiency reasons
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

        // type of n-gram to be mined
        private int type;

        // last n-gram emitted
        private int[] lastContents = new int[0];

        // support of last n-gram emitted
        private int lastSupport = 0;

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 1);
            type = context.getConfiguration().getInt("de.mpii.ngrams.type", NG.ALL);
        }

        @Override
        protected void reduce(IntArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int support = 0;
            for (IntWritable value : values) {
                support += value.get();
            }
            if (support >= minsup) {
                boolean isPrefix = (type == NG.ALL ? false : StringUtils.isPrefix(key.getContents(), lastContents));
                boolean sameCount = (support == lastSupport);
                boolean emit = true;

                // skip if maximal n-grams desired
                if (type == NG.MAXIMAL && isPrefix) {
                    emit = false;
                }

                // skip if closed n-grams desired
                if (type == NG.CLOSED && isPrefix && sameCount) {
                    emit = false;
                }

                if (emit) {
                    outValue.set(support);
                    context.write(key, outValue);

                    // remember array and count
                    lastContents = key.getContents();
                    lastSupport = support;
                }
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

        Job job1 = new Job(getConf());

        // set job name and options
        job1.setJobName("NG-Naive (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 1)");
        job1.setJarByClass(this.getClass());
        job1.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
        job1.getConfiguration().setInt("de.mpii.ngrams.maxlen", maxlen);
        job1.getConfiguration().setInt("de.mpii.ngrams.type", type);

        // set input and output paths        
        SequenceFileInputFormat.setInputPaths(job1, new Path(input + "/seq"));
        SequenceFileOutputFormat.setOutputPath(job1, new Path(output + (type == NG.ALL ? "/result" : "/tmp")));

        // set input and output format        
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set mapper and reducer class
        job1.setMapperClass(MapOne.class);
        job1.setReducerClass(ReduceOne.class);

        // set the number of reducers
        job1.setNumReduceTasks(numred);

        // add dictionary to distributed cache
        for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(input + "/dic"))) {
            if (file.getPath().toString().contains("part")) {
                DistributedCache.addCacheFile(file.getPath().toUri(), job1.getConfiguration());
            }
        }

        // map output classes
        job1.setMapOutputKeyClass(IntArrayWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(IntArrayWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerFirstOnly.class);
        job1.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

        // start job
        job1.waitForCompletion(true);

        if (type != NG.ALL) {

            Job job2 = new Job(getConf());

            // set job name and options
            job2.setJobName("NG-Naive (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 2)");
            job2.setJarByClass(this.getClass());
            job2.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
            job2.getConfiguration().setInt("de.mpii.ngrams.type", type);

            // set input and output paths        
            SequenceFileInputFormat.setInputPaths(job2, new Path(output + "/tmp"));
            SequenceFileOutputFormat.setOutputPath(job2, new Path(output + "/result"));

            // set input and output format        
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);

            // set mapper and reducer class
            job2.setMapperClass(NGClosedMaximal.MapReverse.class);
            job2.setReducerClass(NGClosedMaximal.ReduceReverse.class);

            // set the number of reducers
            job2.setNumReduceTasks(numred);

            // add dictionary to distributed cache
            for (FileStatus file : FileSystem.get(getConf()).listStatus(new Path(input + "/dic"))) {
                if (file.getPath().toString().contains("part")) {
                    DistributedCache.addCacheFile(file.getPath().toUri(), job2.getConfiguration());
                }
            }

            // map output classes
            job2.setMapOutputKeyClass(IntArrayWritable.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(IntArrayWritable.class);
            job2.setOutputValueClass(IntWritable.class);
            job2.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerFirstOnly.class);
            job2.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

            // start job
            job2.waitForCompletion(true);

            // delete temporary directory
            FileSystem.get(getConf()).delete(new Path(output + "/tmp"), true);
        }

        System.err.println("Took " + (System.currentTimeMillis() - start) + " ms");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NGNaive(), args);
        System.exit(exitCode);
    }
}
