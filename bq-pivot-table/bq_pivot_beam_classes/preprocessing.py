import apache_beam as beam


class StateToRegion(beam.DoFn):
    def __init__(self):
        self.regions = {
            'Northeast': ['CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA'],
            'Midwest': ['IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD'],
            'South': ['DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'DC', 'WV', 'AL', 'KY', 'MS', 'TN', 'AR', 'LA', 'OK', 'TX'],
            'West': ['AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA']
            }
    def process(self, element):
        fin = {k:v for k,v in element.items() if k != 'state'}
        for region in self.regions:
            if element['state'] in self.regions[region]:
                # fin.update({'region': region})
                fin.update({'state': region})
                yield fin
        # if 'region' not in fin:
        if 'state' not in fin:
            # fin.update({'region': "other"})
            fin.update({'state': "other"})
            yield fin