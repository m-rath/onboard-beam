
"""A DoFn that reads an Avro file while supporting initial and dynamic splitting. Functions 'split' and 'new_tracker' are optional (can be inferred automatically from restriction type) and are given here for illustration purposes."""

import apache_beam as beam

class AvroReader(beam.DoFn):

  def process(filename, tracker = beam.DoFn.RestrictionTrackerParam):
    with fileio.ChannelFactory.open(filename) as file:
      start, stop = tracker.current_restriction()
      file.seek(start)


      block = AvroUtils.get_next_block(file)


      while block:

        if not tracker.try_claim(block.start()):
          return
        
        for record in block.records():
          yield record
        
        block = AvroUtils.get_next_block(file)


  def get_initial_restriction(self, filename):
    return (0, fileio.ChannelFactory.size_in_bytes(filename))
    

  def split(self, restriction, num_parts):
    start, stop = restriction
    range_size = (stop - start) / num_parts
    current_start = start
    while current_start <= current_stop:
      current_stop = min(current_start + range_size, stop)
      yield (current_start, current_stop)
      current_start = current_stop

  def new_tracker(restriction):
    range_tracker = OffsetRangeTracker(*restriction)
    return RestrictionFromRangeTracker(range_tracker)