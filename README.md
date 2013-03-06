mpiingrams
==========

mpiingrams was created by the Databases and Information Systems Department
at the Max Planck Institute for Informatics (http://www.mpi-inf.mpg.de). It 
provides implementations of different methods to compute n-gram statistics
using Apache Hadoop. For a detailed description of the methods and an
experimental comparison of them on different datasets, please refer to our paper:

Klaus Berberich and Srikanta Bedathur: Computing n-Gram Statistics in MapReduce, In Proceedings of 16th International Conference on Extending Database Technology (EDBT 2013) [PDF](http://www.mpi-inf.mpg.de/~kberberi/publications/2013-edbt2013.pdf)

## Input Conversion

Our implementation expects its input in a specific format that consists of a 
dictionary, mapping words to identifiers and their collection frequency, as 
well as documents compactly encoded as integer sequences.

We provide a tool (`de.mpii.ngrams.io.ConvertText2Input) to convert plain-text
data into this format. This converter removes all non-alphanumeric characters
from the input, treats white spaces as word separators, and newlines as
sentence boundaries.

For other kinds of input data, you'll have to write your own tool to convert it
into the format expected by our implementation.

## Computing n-Gram Statistics

We provide four different implementations (`NGSuffixSigma`, `NGNaive`, `NGAprioriScan`, 
`NGAprioriIndex`) to compute n-gram statistics. We strongly recommend that you use
`NGSuffixSigma`, which has been shown in our experiments to be the most robust
among the methods. All methods expect the same parameters and are invoked as follows:

    hadoop jar de.mpii.ngrams.methods.{NGSuffixSigma, NGNaive, NGAprioriScan, NGAprioriIndex} <input> <output> <minimum_support> <maximum_length> <type> <number_of_reducers>

input : the HDFS path with your dataset converted into our integer-sequence format
output : the HDFS path where you would like to store the computed n-gram statistics
minimum_support : your choice of the minimum support \tau
maximum_length : your choice of the maximum length \sigma (use 0 for non-restricted length)
type : indicates whether you want to compute statistics for all (0), closed (1), or maximal (2) n-grams
number_of_reducers : the number of reducers that you want to use

## Output Conversion

Our implementation outputs n-grams as integer sequences. We provide a tool 
(`de.mpii.ngrams.io.ConvertOutput2Text`) to convert our output to plain text.

## Example

Let's assume you have the sample data provided with this code (the top-5 books 
from Project Gutenberg at the time of writing this documentation) stored in 
./data in your HDFS home directory. To convert it using 4 reducers, use:

    hadoop jar de.mpii.ngrams.io.ConvertText2Input data input 4

To compute statistics for all n-grams that occur at least five times
(i.e., tau = 5) and have length at most ten (i.e., sigma = 10) using our
suffix-based method with 32 reducers, invoke:

    hadoop jar de.mpii.ngrams.methods.NGSuffixSigma input output 5 10 0 4

To convert the computed n-gram statistics to plain text, use:

    hadoop jar de.mpii.ngrams.io.ConvertOutput2Text output input stats

Your statistics are then ready in the ./stats directory and can be viewed using:

    hadoop fs -cat stats/part*

## Citations

If you use our code for research, please cite:

    @inproceedings{Berberich2013,
    author = {Berberich, Klaus and Bedathur, Srikanta},
    title = {{Computing n-Gram Statistics in MapReduce}},
    booktitle = {16th International Conference on Extending Database Technology, EDBT '13, Genua, Italy},
    year = {2013}  
    }

## License

mpiingrams by Max-Planck-Institute for Informatics, Databases 
and Information Systems is licensed under a Creative Commons 
Attribution-NonCommercial-ShareAlike 3.0 Unported License.

## Libraries

Our software uses the following libraries (included in the ./lib directory),
which are both licensed under the Apache 2.0 license (included in the 
./licenses directory):

FastUtil (6.5.2)
http://fastutil.di.unimi.it

MapDB (0.9)
http://www.mapdb.org

The code has been tested on Cloudera CDH3u0. While we expect it to also work
with other distributions and/or newer versions of Apache Hadoop, we haven't 
tested it and cannot provide any support in this direction.