"""A DoFn that takes as input pairs (x, N) and outputs pairs (x, 0), (x, 1), …, (x, N-1)."""
import apache_beam as beam

class CountFn(beam.DoFn):

  def process(element, tracker=beam.DoFn.RestrictionTrackerParam):
    for i in xrange(*tracker.current_restriction()):
      if not tracker.try_claim(i):
        return
      yield element[0], i
        
  def get_initial_restriction(element):
    return (0, element[1])
