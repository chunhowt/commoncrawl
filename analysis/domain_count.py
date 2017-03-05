import sys
import urlparse

from common import CommonJob

from mrjob.step import MRStep

class DomainCount(CommonJob):
  """A MR job to count the # occurrences for domain.
  """
  def configure_options(self):
    super(DomainCount, self).configure_options()
    self.add_passthrough_option(
        '--whitelist_netloc_csv', type='string', default='',
        help='CSV of whitelisted netlocs.')

  def init_whitelist_netloc(self):
    """Initializes whitelisted list of netlocs from options."""
    self.whitelist_netloc = set(self.options.whitelist_netloc_csv.split(','))

  def process_warc_record(self, record):
    """Process each record and output domain, count."""
    # Skip record that is not url page, such as header.
    if record['Content-Type'] != 'text/plain':
      return

    self.increment_counter('commoncrawl', 'num-record', 1)

    url = record.header['warc-target-uri']
    if not url:
      self.increment_counter('commoncrawl', 'num-empty-url', 1)
      return

    netloc = urlparse.urlparse(url).netloc
    if not netloc:
      sys.stderr.write('Empty netloc for url: %s\n' % url)
      self.increment_counter('commoncrawl', 'num-empty-netloc', 1)
      return
 
    # Drop the port number if any.
    netloc = netloc.split(':')[0]
    yield netloc, 1
    self.increment_counter('commoncrawl',  'num-netloc', 1)
    if netloc in self.whitelist_netloc:
      self.increment_counter('commoncrawl', netloc, 1)

  def steps(self):
    return [
      # First, key by domain to get the (domain, count).
      MRStep(
          mapper_init=self.init_whitelist_netloc,
          mapper=self.map_warc_files,
          combiner=self.combiner_sum,
          reducer=self.reduce_sum),
      # Next, grab the topN domains by count. We run the counts through mappers
      # and then a single reducer which works because the key (domain) is
      # already unique by then and we have a single reducer in the end.
      MRStep(
          mapper_init=self.topn_init,
          mapper=self.topn_process,
          mapper_final=self.topn_final,
          reducer_init=self.topn_init,
          reducer=self.topn_process,
          reducer_final=self.topn_final,
          jobconf = {
              "mapred.job.reduce.capacity": 1,
              "mapreduce.job.reduces": 1 })
    ]

if __name__ == '__main__':
  DomainCount.run()
