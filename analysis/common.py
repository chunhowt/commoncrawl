import gzip
import os.path as Path
import sys

import boto
import warc

from boto.s3.key import Key
from gzipstream import GzipStreamFile
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

class CommonJob(MRJob):
  """Based on https://github.com/commoncrawl/cc-mrjob/blob/master/mrcc.py
  """
  HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
  #INPUT_PROTOCOL = RawProtocol

  def configure_options(self):
    super(CommonJob, self).configure_options()
    self.pass_through_option('--runner')
    self.pass_through_option('-r')

  def process_warc_record(self, record):
    """Override process_record with your mapper.

    Args:
      record: A WARC record to be processed.

    Returns:
      Generator of (key, value) tuples.
    """
    raise NotImplementedError('Process record needs to be customized')

  def map_warc_files(self, _, line):
    """Mapper function to process each WARC file.

    Args:
      line: Each line is a path to a WARC gz file to be processed.

    Returns:
      Generator of (key, value) tuples.
    """
    f = None
    # If we are on EC2 or running on a Hadoop cluster, pull files via S3
    if self.options.runner in ['emr', 'hadoop']:
      # Connect to Amazon S3 using anonymous credentials
      conn = boto.connect_s3(anon=True)
      pds = conn.get_bucket('commoncrawl')
      # Start a connection to one of the WARC files
      k = Key(pds, line)
      f = warc.WARCFile(fileobj=GzipStreamFile(k))
    # If we are local, use files on the local file system
    else:
      line = Path.join(Path.abspath(Path.dirname(__file__)), line)
      print 'Loading local file {}'.format(line)
      f = warc.WARCFile(fileobj=gzip.open(line))

    # For each WARC record:
    for i, record in enumerate(f):
      for key, value in self.process_warc_record(record):
        yield key, value
    self.increment_counter('commoncrawl', 'num-files', 1)

  def reduce_sum(self, key, value):
    """Default reducer to sum the values for the key.
    """
    yield key, sum(value)
