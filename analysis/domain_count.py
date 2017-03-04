import sys
import urlparse

from common import CommonJob

from mrjob.step import MRStep

class DomainCount(CommonJob):
  """A MR job to count the # occurrences for domain.
  """

  def process_warc_record(self, record):
    """Process each record and output domain, count.
    """
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
 
    yield netloc.split(':')[0], 1
    self.increment_counter('commoncrawl',  'num-netloc', 1)

  def steps(self):
    return [
      MRStep(
          mapper=self.map_warc_files,
          combiner=self.reduce_sum,
          reducer=self.reduce_sum),
    ]

if __name__ == '__main__':
  DomainCount.run()
