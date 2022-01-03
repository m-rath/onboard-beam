
"""A DoFn that splits files found from a given file-pattern while supporting checkpointing."""

class WatchFilePatternFn(DoFn):
  def process(element, tracker=DoFn.RestrictionTrackerParam,    
              watermark_reporter=DoFn.WatermarkReporterParam):
    file_pattern = element
    claimed_files = tracker.claimed_files()
    num_claimed = len(claimed_files)
    expected_total = fileio.ChannelFactory.expected_files_for_pattern(file_pattern)

    max_timestamp = 0
    for file_stat in fileio.ChannelFactory.expand_glob(file_pattern):
      timestamp, file_name = file_stat
      max_timestamp = max(max_timestamp, timestamp)
      if file_name in claimed_files:
        continue
  
      if num_claimed >= expected_total:
        return DoFn.done()

      if tracker.try_claim(file_name):
        num_claimed += 1
        yield file_name
      else:
        break

    watermark_reporter(max_timestamp)
    yield DoFn.resume(resume_delay=5)

class ClaimedSetTracker(RestrictionTracker):
  def __init__(self):
    self.claimed_files = []
    self.checkpoint_requested = False

  def try_claim(self, file_name):
    if self.checkpoint_requested:
      return False
    claimed_files.append(file_name)
    return True

  def try_split(self, fraction_of_remainder_to_keep):
    if fraction_of_remainder_to_keep == 0:
      self.checkpoint_requested = True
