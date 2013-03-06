package de.mpii.ngrams.methods;

import de.mpii.ngrams.util.PostingWritable;
import de.mpii.ngrams.util.IntArrayWritable;
import de.mpii.ngrams.util.PostingArrayWritable;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mapdb.DBMaker;

/**
 * GSP-style computation of (all|closed|maximal) n-gram frequencies.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class NGAprioriIndex extends Configured implements Tool {

    public static final class MapOne extends Mapper<LongWritable, IntArrayWritable, IntArrayWritable, PostingWritable> {

        // currently considered n-gram length k
        private int k;

        // minimum support threshold
        private int minsup;

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        // singleton output value -- for efficiency reasons
        private final PostingWritable outValue = new PostingWritable();

        // set of frequent terms
        private final IntOpenHashSet tidSet = new IntOpenHashSet();

        // singleton hashmap to collect offsets -- for efficiency reasons
        private Object2ObjectOpenHashMap<IntArrayWritable, IntArrayList> arrayOffsets = new Object2ObjectOpenHashMap<IntArrayWritable, IntArrayList>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 1);
            k = context.getConfiguration().getInt("de.mpii.ngrams.k", 1);
            try {
                HashMap<String, String> dictionaryFiles = new HashMap<String, String>();
                for (Path cachedPath : DistributedCache.getLocalCacheFiles(context.getConfiguration())) {
                    if (cachedPath.toString().contains("dic") && cachedPath.toString().contains("part")) {
                        String file = cachedPath.toString();
                        dictionaryFiles.put(file.substring(file.lastIndexOf("/")), file);
                    }
                }
                ArrayList<String> fileNames = new ArrayList<String>(dictionaryFiles.keySet());
                Collections.sort(fileNames);
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

                // extract all word pairs
                for (int pb = b; pb < e - k + 1; pb++) {
                    outKey.setContents(contents, pb, pb + k);
                    IntArrayList offsets = arrayOffsets.get(outKey);
                    if (offsets == null) {
                        offsets = new IntArrayList();
                        arrayOffsets.put(new IntArrayWritable(outKey.getContents()), offsets);
                    }
                    offsets.add(pb);
                }

                // adjust begin
                b = e;
            }

            // emit postings for this docuemnt
            outValue.setDId(key.get());
            for (IntArrayWritable array : arrayOffsets.keySet()) {
                outValue.setOffsets(arrayOffsets.get(array).toIntArray());
                context.write(array, outValue);
            }

            // clear offsets
            arrayOffsets.clear();
        }
    }

    public static final class ReduceOne extends Reducer<IntArrayWritable, PostingWritable, IntArrayWritable, PostingArrayWritable> {

        // minimum support threshold
        private int minsup;

        // singleton posting list -- for efficiency reasons
        private final ArrayList<PostingWritable> postings = new ArrayList<PostingWritable>();

        // singleton output value -- for efficiency reasons
        private final PostingArrayWritable outValue = new PostingArrayWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 1);
        }

        @Override
        protected void reduce(IntArrayWritable key, Iterable<PostingWritable> values, Context context) throws IOException, InterruptedException {
            int support = 0;
            for (PostingWritable value : values) {
                support += value.getTF();
                postings.add(new PostingWritable(value.getDId(), value.getOffsets()));
            }
            if (support >= minsup) {
                Collections.sort(postings);
                outValue.setTerm(key.getContents());
                outValue.setPostings(postings.toArray(new PostingWritable[0]));
                context.write(key, outValue);
            }
            postings.clear();
        }
    }

    public static final class MapTwo extends Mapper<IntArrayWritable, PostingArrayWritable, IntArrayWritable, PostingArrayWritable> {

        // currently considered n-gram length k
        private int k;

        // contents of output key -- for efficiency reasons
        private int[] outKeyContents;

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("de.mpii.ngrams.k", 1);
            outKeyContents = new int[k];
        }

        @Override
        protected void map(IntArrayWritable key, PostingArrayWritable value, Context context) throws IOException, InterruptedException {
            int[] contents = key.getContents();

            // emit left array (1 as last item)
            System.arraycopy(contents, 0, outKeyContents, 0, contents.length - 1);
            outKeyContents[outKeyContents.length - 1] = 1;
            outKey.setContents(outKeyContents);
            context.write(outKey, value);

            // emit right array (0 as last item)
            System.arraycopy(contents, 1, outKeyContents, 0, contents.length - 1);
            outKeyContents[outKeyContents.length - 1] = 0;
            outKey.setContents(outKeyContents);
            context.write(outKey, value);

        }
    }

    public static final class ReduceTwo extends Reducer<IntArrayWritable, PostingArrayWritable, IntArrayWritable, PostingArrayWritable> {

        // minimum support threshold
        private int minsup;

        // currently considered n-gram length k
        private int k;

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        // singleton output value -- for efficiency reasons
        private final PostingArrayWritable outValue = new PostingArrayWritable();

        // posting-list buffer
        private Set<PostingArrayWritable> set = DBMaker.newTempTreeSet();

        // currently considered join key
        private int[] joinKey;

        // singleton hashmap over right posting lists -- for efficiency reasons
        private final Long2ObjectOpenHashMap<PostingWritable> rightIndex = new Long2ObjectOpenHashMap<PostingWritable>();

        // singleton array list to collect offsets -- for efficiency reasons
        private final IntArrayList offsets = new IntArrayList();

        // singleton list to collect new postings -- for efficiency reasons
        private final ArrayList<PostingWritable> postings = new ArrayList<PostingWritable>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 1);
            k = context.getConfiguration().getInt("de.mpii.ngrams.k", 1);

            // initialize key
            joinKey = new int[k];
        }

        @Override
        protected void reduce(IntArrayWritable key, Iterable<PostingArrayWritable> values, Context context) throws IOException, InterruptedException {
            int[] contents = key.getContents();
            if (contents[contents.length - 1] == 1) { // right join partner = left array (1 as last item)
                set.clear();
                for (PostingArrayWritable value : values) {
                    set.add(new PostingArrayWritable(value.getTerm().clone(), value.getPostings().clone()));
                }
                System.arraycopy(contents, 0, joinKey, 0, contents.length - 1);
                context.progress();
            } else if (contents[contents.length - 1] == 0) { // left join partner = right array (0 as last item)
                // check that these are the correct join partners
                boolean join = true;
                for (int i = 0; i < joinKey.length && join; i++) {
                    join = (joinKey[i] == contents[i]);
                }
                if (join) {
                    for (PostingArrayWritable left : values) {
                        PostingWritable[] leftPostings = left.getPostings();

                        for (PostingArrayWritable right : set) {
                            PostingWritable[] rightPostings = right.getPostings();

                            // try to avoid join if possible
                            if (leftPostings[0].getDId() > rightPostings[rightPostings.length - 1].getDId()
                                    || rightPostings[0].getDId() > leftPostings[leftPostings.length - 1].getDId()) {
                                continue;
                            }

                            // build hashmap of right postings                                                        
                            for (PostingWritable rightPosting : rightPostings) {
                                rightIndex.put(rightPosting.getDId(), rightPosting);
                            }

                            // join right and left postings
                            int support = 0;
                            for (PostingWritable leftPosting : leftPostings) {
                                PostingWritable rightPosting = rightIndex.get(leftPosting.getDId());
                                if (rightPosting != null) {
                                    int[] leftOffsets = leftPosting.getOffsets();
                                    int[] rightOffsets = rightPosting.getOffsets();

                                    if (leftOffsets[leftOffsets.length - 1] + 1 < rightOffsets[0]) {
                                        continue;
                                    }

                                    for (int lpos = 0, rpos = 0, ln = leftOffsets.length, rn = rightOffsets.length; lpos < ln && rpos < rn; lpos++) {

                                        // fast forward right cursor
                                        while (rpos < rn && rightOffsets[rpos] <= leftOffsets[lpos]) {
                                            rpos++;
                                        }

                                        // check whether join condition is met
                                        if (rpos < rn && leftOffsets[lpos] + 1 == rightOffsets[rpos]) {
                                            offsets.add(leftOffsets[lpos]);
                                        }
                                    }

                                    if (!offsets.isEmpty()) {
                                        postings.add(new PostingWritable(leftPosting.getDId(), offsets.toIntArray()));
                                        support += offsets.size();
                                        offsets.clear();
                                    }
                                }
                            }

                            if (support >= minsup) {
                                int[] leftTerm = left.getTerm();
                                int[] rightTerm = right.getTerm();
                                int[] term = new int[k];
                                System.arraycopy(leftTerm, 0, term, 0, leftTerm.length);
                                term[term.length - 1] = rightTerm[rightTerm.length - 1];
                                outKey.setContents(term);
                                outValue.setTerm(term);
                                outValue.setPostings(postings.toArray(new PostingWritable[0]));
                                context.write(outKey, outValue);
                            }

                            postings.clear();
                            rightIndex.clear();
                        }

                        context.progress();
                    }
                }
            }
        }
    }

    public static final class MapThree extends Mapper<IntArrayWritable, PostingArrayWritable, IntArrayWritable, IntWritable> {

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void map(IntArrayWritable key, PostingArrayWritable value, Context context) throws IOException, InterruptedException {
            int support = 0;
            for (PostingWritable posting : value.getPostings()) {
                support += posting.getTF();
            }
            outValue.set(support);
            context.write(key, outValue);
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

        ///
        /// Jobs 1-3: Compute {1,2,3}-grams
        ///

        boolean done = false;
        int k = 1;
        while (k <= 4 && !done) {

            // run first job
            Job job1 = new Job(getConf());

            // set job name and options
            job1.setJobName("NG-AprioriIndex (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 1, Length " + k + ")");
            job1.setJarByClass(this.getClass());
            job1.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
            job1.getConfiguration().setInt("de.mpii.ngrams.maxlen", maxlen);
            job1.getConfiguration().setInt("de.mpii.ngrams.type", type);
            job1.getConfiguration().setInt("de.mpii.ngrams.k", k);

            // set input and output paths        
            SequenceFileInputFormat.setInputPaths(job1, new Path(input + "/seq"));
            SequenceFileOutputFormat.setOutputPath(job1, new Path(output + "/index/" + k));

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
            job1.setMapOutputValueClass(PostingWritable.class);
            job1.setOutputKeyClass(IntArrayWritable.class);
            job1.setOutputValueClass(PostingArrayWritable.class);
            job1.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerComplete.class);
            job1.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

            // start job
            job1.waitForCompletion(true);

            // check whether any records were output
            done = (job1.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue() == 0 || k == maxlen);

            k++;
        }

        ///
        /// Jobs 5-k: Compute {5,..,k}-grams
        ///

        while (!done) {
            Job job2 = new Job(getConf());

            // set job name and options
            job2.setJobName("NG-AprioriIndex (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 2, Length " + k + ")");
            job2.setJarByClass(this.getClass());
            job2.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
            job2.getConfiguration().setInt("de.mpii.ngrams.type", type);
            job2.getConfiguration().setInt("de.mpii.ngrams.k", k);

            // set input and output paths        
            SequenceFileInputFormat.setInputPaths(job2, new Path(output + "/index/" + (k - 1)));
            SequenceFileOutputFormat.setOutputPath(job2, new Path(output + "/index/" + k));

            // set input and output format        
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);

            // set mapper and reducer class
            job2.setMapperClass(MapTwo.class);
            job2.setReducerClass(ReduceTwo.class);

            // set the number of reducers
            job2.setNumReduceTasks(numred);

            // map output classes
            job2.setMapOutputKeyClass(IntArrayWritable.class);
            job2.setMapOutputValueClass(PostingArrayWritable.class);
            job2.setOutputKeyClass(IntArrayWritable.class);
            job2.setOutputValueClass(PostingArrayWritable.class);
            job2.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerAllButLast.class);
            job2.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

            // start job
            job2.waitForCompletion(true);

            // check whether any records were output or maximum length reached
            done = (job2.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue() == 0 || k == maxlen);

            k++;
        }

        ///
        /// Job (k+1): Convert index to canonical output format
        ///

        // set job name and options
        Job job3 = new Job(getConf());
        job3.setJobName("NG-AprioriIndex (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 3)");
        job3.setJarByClass(this.getClass());

        // set input and output paths        
        SequenceFileInputFormat.setInputPaths(job3, new Path(output + "/index/*/part*"));
        SequenceFileOutputFormat.setOutputPath(job3, new Path(output + (type == NG.ALL ? "/result" : "/tmp")));

        // set input and output format   
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set mapper class
        job3.setMapperClass(MapThree.class);

        // set the number of reducers
        job3.setNumReduceTasks(0);

        // map output classes
        job3.setMapOutputKeyClass(IntArrayWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(IntArrayWritable.class);
        job3.setOutputValueClass(IntWritable.class);

        // start job
        job3.waitForCompletion(true);

        FileSystem.get(getConf()).delete(new Path(output + "/index"), true);

        ///
        /// Job (k+2 & k+3): Compute closed/maximal n-grams
        ///

        if (type != NG.ALL) {

            // forward
            Job job4 = new Job(getConf());

            // set job name and options
            job4.setJobName("NG-AprioriIndex (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 4)");
            job4.setJarByClass(this.getClass());
            job4.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
            job4.getConfiguration().setInt("de.mpii.ngrams.type", type);

            // set input and output paths        
            SequenceFileInputFormat.setInputPaths(job4, new Path(output + "/tmp/part*"));
            SequenceFileOutputFormat.setOutputPath(job4, new Path(output + "/forward"));

            // set input and output format        
            job4.setInputFormatClass(SequenceFileInputFormat.class);
            job4.setOutputFormatClass(SequenceFileOutputFormat.class);

            // set mapper and reducer class
            job4.setMapperClass(NGClosedMaximal.MapForward.class);
            job4.setReducerClass(NGClosedMaximal.ReduceForward.class);

            // set the number of reducers
            job4.setNumReduceTasks(numred);

            // map output classes
            job4.setMapOutputKeyClass(IntArrayWritable.class);
            job4.setMapOutputValueClass(IntWritable.class);
            job4.setOutputKeyClass(IntArrayWritable.class);
            job4.setOutputValueClass(IntWritable.class);
            job4.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerFirstOnly.class);
            job4.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

            // start job
            job4.waitForCompletion(true);

            // backward
            Job job5 = new Job(getConf());

            // set job name and options
            job5.setJobName("NG-AprioriIndex (" + minsup + ", " + maxlen + ") (" + type + ") (Phase 5)");
            job5.setJarByClass(this.getClass());
            job5.getConfiguration().setInt("de.mpii.ngrams.minsup", minsup);
            job5.getConfiguration().setInt("de.mpii.ngrams.type", type);

            // set input and output paths        
            SequenceFileInputFormat.setInputPaths(job5, new Path(output + "/forward"));
            SequenceFileOutputFormat.setOutputPath(job5, new Path(output + "/result"));

            // set input and output format        
            job5.setInputFormatClass(SequenceFileInputFormat.class);
            job5.setOutputFormatClass(SequenceFileOutputFormat.class);

            // set mapper and reducer class
            job5.setMapperClass(NGClosedMaximal.MapReverse.class);
            job5.setReducerClass(NGClosedMaximal.ReduceReverse.class);

            // set the number of reducers
            job5.setNumReduceTasks(numred);

            // map output classes
            job5.setMapOutputKeyClass(IntArrayWritable.class);
            job5.setMapOutputValueClass(IntWritable.class);
            job5.setOutputKeyClass(IntArrayWritable.class);
            job5.setOutputValueClass(IntWritable.class);
            job5.setPartitionerClass(IntArrayWritable.IntArrayWritablePartitionerFirstOnly.class);
            job5.setSortComparatorClass(IntArrayWritable.DefaultComparator.class);

            // start job
            job5.waitForCompletion(true);

            // delete temporary directory
            FileSystem.get(getConf()).delete(new Path(output + "/tmp"), true);
            FileSystem.get(getConf()).delete(new Path(output + "/forward"), true);
        }

        System.err.println("Took " + (System.currentTimeMillis() - start) + " ms");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NGAprioriIndex(), args);
        System.exit(exitCode);
    }
}
