#!/usr/bin/python

import musicsql

class Query(musicsql.Query):

    def table_data(self):
        self.requires = ['upbeat', 'quality']
        self.foreignkey['moment_id'] = 'moments'

    def sql(self):
		part = self.part()
		moment1 = part.add_moment()
		moment2 = moment1.add_later_moment()
		moment2.select_alias('row_id', 'moment_id')
        
		upbeat = moment2.node('upbeat')
		moment1.add_constraint(upbeat.c['upbeatmoment_id'] == moment1.c['row_id'])

		quality2 = moment2.node('quality')
		q2 = quality2.c['quality']
		q2_stable = self.conditional()
		q2_stable.IF(q2 == 'major').OR(q2 == 'minor').OR(q2 == 'fifth').OR(q2 == "major third").OR(q2 == 'minor third').OR(q2 == 'unison')
		moment2.add_constraint(q2_stable)
		
		quality1 = moment1.node('quality')
		q1 = quality1.c['quality']
		motion = (quality2.c['root'] - quality1.c['root'] + 84) % 12
		major = self.conditional().IF(q1 == 'major').OR(q1.like('dominant%')).AND(motion == 5)
		halfdim = self.conditional().IF(q1 == 'half-diminished').AND(motion == 1)
		dim = self.conditional().IF(motion == 1).OR(motion == 4).OR(motion == 7).AND(q1 == 'diminished')
		fulldim = self.conditional().IF(motion == 1).OR(motion == 4).OR(motion == 7).OR(motion == 10).AND(q1 == 'fully-diminished')
		rootmotion = self.conditional().IF(major).OR(halfdim).OR(dim).OR(fulldim)
		moment1.add_constraint(rootmotion)

if __name__ == '__main__':
    query = Query()
    query.run(printing=True)
