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
			FileSystem fs = FileSystem.get(context.getConfiguration());

			// TODO get the cache files from the context
			URI[] uris = context.getCacheFiles();

			if (uris.length > 0) {
				// TODO create a new Bloom filter
				filter = new BloomFilter();
				
				// TODO call the filter's readFields method, passing in an FSDataInputStream
				filter.readFields(fs.open(new Path(uris[0].toString())));
			} else {
				throw new IOException(
						"Bloom filter file not in DistributedCache");
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// TODO split the input key on tabs
			tokens = value.toString().trim().split("\t", 2);
			
			// TODO set the bfKey's bytes to the 0-th token (username), weight of 1.0
			bfKey.set(tokens[0].getBytes(), 1.0);

			// TODO if the filter's membership test passes
			if (filter.membershipTest(bfKey)) {
				// TODO write the input value as the key with a NullWritable value
				context.write(value, NullWritable.get());
			}
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
		Job job = Job.getInstance(getConf(), "Bloom Filtering");
		job.setJarByClass(MRBloomFilter.class);

		// TODO add the BLoom URI file to the cache
		job.addCacheFile(bloom);

		// TODO set the mapper class
		job.setMapperClass(BloomMapper.class);

		// TODO set the number of reduce tasks to 0
		job.setNumReduceTasks(0);

		// TODO set the input paths
		TextInputFormat.setInputPaths(job, input);

		// TODO set the output paths
		TextOutputFormat.setOutputPath(job, output);

		// TODO set the output key class to Text
		job.setOutputKeyClass(Text.class);

		// TODO set the output value class to NullWritable
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MRBloomFilter(), args);
	}
}