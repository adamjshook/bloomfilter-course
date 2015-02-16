# bloomfilter-course
Course material on the beautiful and glorious world of Bloom filters!

This course covers some basics of Bloom filters and includes a few exercises.

The exercises are as follows:
+Trainer - Trains a Bloom filter and populates a Redis set
+Tester - Tests a larger data set against Redis, then once again using a Bloom filter
+MRBloomFilter - Uses a Bloom filter in a MapReduce context to filter data

This course requires Redis and can be downloaded from redis.io

The following should be sufficient to get started and build Redis if you are using Linux.  If you are using a Mac, simply download the latest stable release instead of using wget.

```bash
$ tar xzf redis-stable.tar.gz
$ cd redis-stable
$ make

$ src/redis-server
```
Run gen-twitter.py to randomly generate three files used in the exercises

```bash
$ python gen-twitter.py 1000000
```

The exercises include .classpath and .project files for Eclipse, which can be downloaded from [http://www.eclipse.org].

