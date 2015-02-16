package bloomfilter;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class MRBloomFilter extends Configured implements Tool {

	public static class BloomMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private BloomFilter filter = null;
		private String[] tokens = null;
		private Key bfKey = new Key();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			// TODO Create a FileSystem object

			// TODO get the cache files from the context
			URI[] uris = null;

			if (uris.length > 0) {
				// TODO create a new Bloom filter
				
				// TODO call the filter's readFields method, passing in an FSDataInputStream
			} else {
				throw new IOException(
						"Bloom filter file not in DistributedCache");
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// TODO split the input key on tabs
			
			// TODO set the bfKey's bytes to the 0-th token (username), weight of 1.0

			// TODO if the filter's membership test passes
				// TODO write the input value as the key with a NullWritable value
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: Tester <input> <bloom filter> <output>");
			System.exit(1);
		}

		Path input = new Path(args[0]);
		URI bloom = new URI(args[1]);
		Path output = new Path(args[2]);

		// TODO create the Job object, and set the jar by class

		// TODO add the Bloom URI file to the cache

		// TODO set the mapper class

		// TODO set the number of reduce tasks to 0

		// TODO set the input paths

		// TODO set the output paths

		// TODO set the output key class to Text

		// TODO set the output value class to NullWritable

		// TODO execute the job via wait for completion and return 0 if successful
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MRBloomFilter(), args);
	}
}