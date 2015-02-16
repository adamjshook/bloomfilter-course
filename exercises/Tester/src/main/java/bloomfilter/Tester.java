package bloomfilter;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import redis.clients.jedis.Jedis;

public class Tester extends Configured implements Tool {

	public static final String REDIS_SET_KEY = "users";
	
	private Jedis jedis = null;

	public void redisMembershipTest(Path input) throws Exception {

		System.out.println("Testing Redis set membership of " + input);
		
		// TODO create a fileSystem object
		
		// TODO connect to Redis at localhost, port 6379

		// TODO Open the testing file for read
		String line = null;
		int numhits = 0, numlines = 0;
		BufferedReader rdr = null;

		long start = System.currentTimeMillis();
		while ((line = rdr.readLine()) != null) {
			// TODO increment numlines
			
			// TODO Test the line using jedis 'sismember' function
				// TODO increment numhits
			
		}
		long finish = System.currentTimeMillis();

		// TODO Close reader and disconnect Jedis
		
		System.out.println("Took " + (finish - start) + " ms to check Redis "
				+ numlines + " times for " + numhits + " successful tests");

	}

	public void redisMembershipTestWithFilter(Path input, Path bloom)
			throws Exception {

		System.out.println("Testing Redis set membership of " + input
				+ " using a Bloom filter " + bloom);
		
		// TODO create a fileSystem object
		
		// TODO connect to Redis at localhost, port 6379
		
		// TODO Create a new BloomFilter object
		
		// TODO call readFields with an FSDataInputStream from the file

		// TODO Open the testing file for read
		String line = null;
		int numBFhits = 0, numhits = 0, numlines = 0;
		BufferedReader rdr = null;
		
		// TODO create a new Key to re-use
		Key key = new Key();

		long start = System.currentTimeMillis();
		while ((line = rdr.readLine()) != null) {
			// TODO increment numlines
			
			// TODO set the bytes of the key to line's bytes with a weight of 1.0
			
			// TODO membership test the key
				// TODO increment numBFhits

				// TODO test jedis using sismember
					// TODO increment numhits
		}
		long finish = System.currentTimeMillis();

		// TODO close the file reader and Redis client

		System.out.println("Took " + (finish - start) + " ms to check Redis "
				+ numlines + " times for " + numhits
				+ " successful tests.  Bloom filter hits: " + numBFhits
				+ " False postives: " + (numBFhits - numhits));
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: Tester <file to test> <bloom filter>");
			System.exit(1);
		}

		Path input = new Path(args[0]);
		Path bloom = new Path(args[1]);
		
		// First, test against Redis
		redisMembershipTest(input);
		
		// Then, test again using a Bloom filter
		redisMembershipTestWithFilter(input, bloom);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Tester(), args);
	}
}