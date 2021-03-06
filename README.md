## About
This project explores processing the data provided by commoncrawl using Amazon EMR.
http://commoncrawl.org/about/

Example commoncrawl data can be found here: 
https://github.com/commoncrawl/cc-mrjob

## Pre-requisite
Install a bunch of packages needed for this code.

```sh
sudo pip install boto3
sudo pip install mrjob
sudo pip install warc
sudo pip install https://github.com/commoncrawl/gzipstream/archive/master.zip
```

## Running
Eg of running job locally:
```sh
python analysis/domain_count.py --no-output --output-dir out input/example.paths.gz
```

Eg of running job on EMR for testing (using just the master):
```sh
python analysis/domain_count.py -r emr --conf-path analysis/mrjob.conf --num-core-instances=0 --no-output --output-dir $OUTPUT_DIR $INPUT_FILEPATH
```

For full run, change the num of core instances appropriately:
```sh
python analysis/domain_count.py -r emr --conf-path analysis/mrjob.conf --num-core-instances=5 --no-output --output-dir $OUTPUT_DIR $INPUT_FILEPATH
```

Example of $INPUT_FILEPATH is:

s3://commoncrawl/crawl-data/CC-MAIN-2017-04/wet.paths.gz

NOTE: As of 3/4/2017, the gz version might not work due to org.apache.hadoop.mapred.lib.NLineInputFormat not able to deal with compressed file.

NOTE: As of 3/4/2017, I used 5 c3.xlarge core instances for 1 hour to process a sample of 500 wet.paths (out of about 57800 paths).
