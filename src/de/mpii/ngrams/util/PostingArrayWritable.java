package de.mpii.ngrams.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents an array of PostingWritables together with its associated term.
 * Serialization uses gap- and variable-byte encoding, assuming that postings in
 * the array are sorted in ascending order of their document identifier.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class PostingArrayWritable implements Writable, Serializable, Comparable {

    // term identifier associated with this posting array
    private int[] term = new int[0];

    // posting array
    private PostingWritable[] postings;

    public PostingArrayWritable() {
        postings = new PostingWritable[0];
    }

    public PostingArrayWritable(PostingWritable[] postings) {
        this.postings = postings;
    }

    public PostingArrayWritable(int[] term, PostingWritable[] postings) {
        this.term = term;
        this.postings = postings;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(term + " : ");
        for (PostingWritable posting : postings) {
            sb.append(posting + " ");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeVInt(d, term.length);
        for (int i = 0; i < term.length; i++) {
            WritableUtils.writeVInt(d, term[i]);
        }
        WritableUtils.writeVInt(d, postings.length);
        for (int i = 0; i < postings.length; i++) {
            PostingWritable posting = postings[i];
            if (i == 0) {
                WritableUtils.writeVLong(d, posting.getDId());
            } else {
                WritableUtils.writeVLong(d, posting.getDId() - postings[i - 1].getDId());
            }
            int[] offsets = posting.getOffsets();
            WritableUtils.writeVInt(d, offsets.length);
            for (int j = 0; j < offsets.length; j++) {
                if (j == 0) {
                    WritableUtils.writeVInt(d, offsets[j]);
                } else {
                    WritableUtils.writeVInt(d, offsets[j] - offsets[j - 1]);
                }
            }
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        term = new int[WritableUtils.readVInt(di)];
        for (int i = 0; i < term.length; i++) {
            term[i] = WritableUtils.readVInt(di);
        }
        postings = new PostingWritable[WritableUtils.readVInt(di)];
        for (int i = 0; i < postings.length; i++) {
            long did = (i == 0 ? 0 : postings[i - 1].getDId());
            did += WritableUtils.readVLong(di);
            int[] offsets = new int[WritableUtils.readVInt(di)];
            for (int j = 0; j < offsets.length; j++) {
                offsets[j] = (j == 0 ? 0 : offsets[j - 1]);
                offsets[j] += WritableUtils.readVInt(di);
            }
            postings[i] = new PostingWritable(did, offsets);
        }
    }

    /**
     * Sets the term associated with this posting array.
     */
    public void setTerm(int[] term) {
        this.term = term;
    }

    /**
     * Sets the posting array.
     */
    public void setPostings(PostingWritable[] postings) {
        this.postings = postings;
    }

    /**
     * Gets the term associated with this posting array.
     */
    public int[] getTerm() {
        return term;
    }

    /**
     * Gets the posting array.
     */
    public PostingWritable[] getPostings() {
        return postings;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(term);
    }

    @Override
    public boolean equals(Object other) {
        int[] otherTerm = ((PostingArrayWritable) other).term;
        return Arrays.equals(term, otherTerm);
    }

    @Override
    public int compareTo(Object other) {
        int[] otherTerm = ((PostingArrayWritable) other).term;
        int length = term.length;
        int otherLength = otherTerm.length;
        int minLength = (length < otherLength ? length : otherLength);
        for (int i = 0; i < minLength; i++) {
            int tid = term[i];
            int otherTId = otherTerm[i];
            if (tid < otherTId) {
                return +1;
            } else if (tid > otherTId) {
                return -1;
            }
        }
        return (otherLength - length);
    }
}
