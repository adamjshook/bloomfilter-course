# bloomfilter-course
Course material on the beautiful and glorious world of Bloom filters!


This course covers some basics of Bloom filters and includes a few exercises.

The exercises are as follows:
    Trainer - Trains a Bloom filter and populates a Redis set
    Tester - Tests a larger data set against Redis, then once again using a Bloom filter
    MRBloomFilter - Uses a Bloom filter in a MapReduce context to filter data

This course requires Redis and can be downloaded from redis.io

The following should be sufficient to get started and build Redis if you are using Linux.  If you are using a Mac, simply download the latest stable release instead of using wget.

You'll need some development libraries installed to get it to build properly.

$ wget http://download.redis.io/redis-stable.tar.gz
$ tar xzf redis-stable.tar.gz
$ cd redis-stable
$ make

$ src/redis-server

