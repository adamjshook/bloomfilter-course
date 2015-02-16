package bloomfilter;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import redis.clients.jedis.Jedis;

public class Trainer extends Configured implements Tool {

	public static final String REDIS_SET_KEY = "users";
	private Jedis jedis = null;

	public BloomFilter createBloomFilter(int numMembers, float falsePosRate) {
		// TODO calculate the optimal Bloom filter size
		// TODO and the optimal number of hash functions
		int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
		int nbHash = getOptimalK(numMembers, vectorSize);

		// TODO create new Bloom filter
		BloomFilter filter = new BloomFilter(vectorSize, nbHash,
				Hash.MURMUR_HASH);
		
		return filter;
	}
	
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 4) {
			System.err
					.println("Usage: Trainer <totrain> <nummembers> <falseposrate> <bfoutfile>");
			return 1;
		}

		// Parse command line arguments
		Path inputFile = new Path(args[0]);
		int numMembers = Integer.parseInt(args[1]);
		float falsePosRate = Float.parseFloat(args[2]);
		Path bfFile = new Path(args[3]);
		
		// TODO Create a new Jedis object using localhost at port 6379
		jedis = new Jedis("localhost", 6379);
		
		// TODO delete the REDIS_SET_KEY
		jedis.del(REDIS_SET_KEY);

		// TODO Create a new Bloom filter
		BloomFilter filter = createBloomFilter(numMembers, falsePosRate);

		// TODO open the file for read
		FileSystem fs = FileSystem.get(getConf());
		String line = null;
		int numRecords = 0;
		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				fs.open(inputFile)));

		while ((line = rdr.readLine()) != null) {
			// TODO if the line is not empty
			if (!line.isEmpty()) {
				// TODO add the line to the Bloom filter
				filter.add(new Key(line.getBytes()));
				
				// TODO use Jedis client's "sadd" method to set
				jedis.sadd(REDIS_SET_KEY, line);
				++numRecords;
			}
		}

		// TODO Close reader, disconnect Jedis client
		rdr.close();
		jedis.disconnect();

		System.out.println("Trained Bloom filter with " + numRecords
				+ " entries.");

		System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
		
		// TODO create anew FSDataOutputStream using the FileSystem
		FSDataOutputStream strm = fs.create(bfFile);
		
		// TODO pass the stream to the Bloom filter
		filter.write(strm);

		// TODO close the stream
		strm.flush();
		strm.close();

		System.out.println("Done training Bloom filter.");
		return 0;
	}

	public static int getOptimalBloomFilterSize(int numRecords,
			float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
				.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Trainer(), args);
	}
}