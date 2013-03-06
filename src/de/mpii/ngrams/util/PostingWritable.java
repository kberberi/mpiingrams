package de.mpii.ngrams.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents a positional posting that indicates at which <i>offsets</i> a
 * specific term occur in the document with identifier <i>did</i>.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class PostingWritable implements WritableComparable<PostingWritable>, Serializable {

    // document identifier
    private long did;

    // offsets
    private int[] offsets;

    public PostingWritable() {
        did = 0;
        offsets = new int[0];
    }

    public PostingWritable(long did, int[] offsets) {
        this.did = did;
        this.offsets = offsets;
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        did = WritableUtils.readVLong(di);
        offsets = new int[WritableUtils.readVInt(di)];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = (i == 0 ? WritableUtils.readVInt(di) : offsets[i - 1] + WritableUtils.readVInt(di));
        }
    }

    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeVLong(d, did);
        WritableUtils.writeVInt(d, offsets.length);
        for (int i = 0; i < offsets.length; i++) {
            if (i == 0) {
                WritableUtils.writeVInt(d, offsets[i]);
            } else {
                WritableUtils.writeVInt(d, offsets[i] - offsets[i - 1]);
            }
        }
    }

    /**
     * Gets document identifier.
     */
    public long getDId() {
        return did;
    }

    /**
     * Gets term frequency.
     */
    public int getTF() {
        return offsets.length;
    }

    /**
     * Gets offsets.
     */
    public int[] getOffsets() {
        return offsets;
    }

    /**
     * Sets document identifier.
     */
    public void setDId(long did) {
        this.did = did;
    }

    /**
     * Sets offsets.
     */
    public void setOffsets(int[] offsets) {
        this.offsets = offsets;
    }

    @Override
    public String toString() {
        return "( " + did + " | " + offsets.length + " | " + Arrays.toString(offsets) + " )";
    }

    @Override
    public int compareTo(PostingWritable t) {
        if (did < t.did) {
            return -1;
        } else if (did > t.did) {
            return +1;
        }
        return 0;
    }
}
