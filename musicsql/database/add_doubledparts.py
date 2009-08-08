#!/usr/bin/python

import musicsql

class Query(musicsql.Function):

	def table_data(self):
		self.requires = ()
		self.foreignkey = ('moment_id', 'moments')
		self.field_types['moment_id'] = self.types['integer']
		self.distinct = True

	def function(self, row):
		'''
		function is run for each row of a result
		- return a list of fields corresponding to the 'newFields'
		  established in the run function
		'''
		self.add_output([('moment_id', row['momentA_id'])])
		self.add_output([('moment_id', row['momentB_id'])])
		self.add_output([('moment_id', row['momentC_id'])])

	def sql(self):
		part1 = self.part()
		part2 = self.part()

		momentA = part1.add_moment()
		note1A = momentA.add_note(part1)
		note2A = momentA.add_note(part2)
		note1A.add_constraint(note1A.c['row_id'] > note2A.c['row_id'])
		note1A.add_constraint((note1A.c['semit'] - note2A.c['semit'] + 84) % 12 == 0)
		momentA.select_alias('row_id', 'momentA_id')

		note1B = note1A.add_next_semit()
		note2B = note2A.add_next_semit()
		note1B.add_constraint((note1B.c['semit'] - note2B.c['semit'] + 84) % 12 == 0)
		moment1B = note1B.start_moment()
		moment1B.select_alias('row_id', 'momentB_id')

		note1C = note1B.add_next_semit()
		note2C = note2B.add_next_semit()
		note1C.add_constraint((note1C.c['semit'] - note2C.c['semit'] + 84) % 12 == 0)
		moment1C = note1C.start_moment()
		moment1C.select_alias('row_id', 'momentC_id')

if __name__ == '__main__':
	query = Query()
	query.run(printing=True)
