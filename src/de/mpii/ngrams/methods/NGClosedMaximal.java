package de.mpii.ngrams.methods;

import de.mpii.ngrams.util.IntArrayWritable;
import de.mpii.ngrams.util.StringUtils;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Mappers and reducers employed by all methods to compute closed/maximal
 * n-grams.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class NGClosedMaximal {

    /**
     * Identity mapper.
     */
    public static final class MapForward extends Mapper<IntArrayWritable, IntWritable, IntArrayWritable, IntWritable> {

        @Override
        protected void map(IntArrayWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    /**
     * Reducer that expects n-grams in reverse lexicographic order. It emits an
     * n-gram only if it is no prefix of the previously emitted n-gram (maximal)
     * and it its support differs from the support of the previously emitted
     * n-gram (closed).
     */
    public static final class ReduceForward extends Reducer<IntArrayWritable, IntWritable, IntArrayWritable, IntWritable> {

        // type of n-grams to be mined
        private int type;

        // last n-gram emited
        private int[] lastContents = new int[0];

        // support of last n-gram emitted
        private int lastSupport = 0;

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            type = context.getConfiguration().getInt("de.mpii.ngrams.type", NG.ALL);
        }

        @Override
        protected void reduce(IntArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int[] contents = key.getContents();

            int support = values.iterator().next().get();

            boolean isPrefix = (type == NG.ALL ? false : StringUtils.isPrefix(contents, lastContents));
            boolean sameCount = (support == lastSupport);

            // skip if maximal n-grams desired
            if (type == NG.MAXIMAL && isPrefix) {
                return;
            }

            // skip if closed n-grams desired
            if (type == NG.CLOSED && isPrefix && sameCount) {
                return;
            }

            outKey.setContents(contents);
            outValue.set(support);
            context.write(outKey, outValue);

            lastContents = contents;
            lastSupport = support;
        }
    }

    /**
     * Mapper reverses n-grams and emits them with their support.
     */
    public static final class MapReverse extends Mapper<IntArrayWritable, IntWritable, IntArrayWritable, IntWritable> {

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        @Override
        protected void map(IntArrayWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            int[] contents = key.getContents();

            // reverse contents in-place
            for (int i = 0, n = contents.length, m = (n >> 1); i < m; i++) {
                int tmp = contents[i];
                contents[i] = contents[n - i - 1];
                contents[n - i - 1] = tmp;
            }

            outKey.setContents(contents);
            context.write(outKey, value);
        }
    }

    /**
     * Reducer that expects reversed n-gram in reverse lexicographic order. It
     * only emits a reversed n-gram if it is no prefix of the previously emitted
     * reversed n-gram (maximal) and its support differs from the support of the
     * previously emitted reversed n-gram (closed). Before emitting a reversed
     * n-gram, it is once more reverse to restore the original order.
     */
    public static final class ReduceReverse extends Reducer<IntArrayWritable, IntWritable, IntArrayWritable, IntWritable> {

        // type of n-grams to be mined
        private int type;

        // last n-gram emitted
        private int[] lastContents = new int[0];

        // support of last n-gram emitted
        private int lastSupport = 0;

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            type = context.getConfiguration().getInt("de.mpii.ngrams.type", NG.ALL);
        }

        @Override
        protected void reduce(IntArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int[] contents = key.getContents();

            // reverse contents in-place 
            for (int i = 0, n = contents.length, m = (n >> 1); i < m; i++) {
                int tmp = contents[i];
                contents[i] = contents[n - i - 1];
                contents[n - i - 1] = tmp;
            }

            int support = values.iterator().next().get();

            boolean isSuffix = StringUtils.isSuffix(contents, lastContents);
            boolean sameCount = (support == lastSupport);

            // skip if maximal n-grams desired
            if (type == NG.MAXIMAL && isSuffix) {
                return;
            }

            // skip if closed n-grams desired
            if (type == NG.CLOSED && isSuffix && sameCount) {
                return;
            }

            outKey.setContents(contents);
            outValue.set(support);
            context.write(outKey, outValue);

            lastContents = contents;
            lastSupport = support;
        }
    }

}
