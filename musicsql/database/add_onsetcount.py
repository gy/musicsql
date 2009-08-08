#!/usr/bin/python

import musicsql

class Query(musicsql.Aggregate):

	def table_data(self):
		self.requires = ()
		self.groupFields = ['moment_id']
			# list of field names used to group aggregates 
		self.field_types['onsetcount'] = self.types['integer']
		self.foreignkey = ('moment_id', 'moments')
			# add a foreign key to connect the new table to one of the
			# main hub tables (moments, notes, parts, noteheads)

	def init(self):
		'''
		init is run at the start of each new aggregate
		- set up the state variable
		'''
		self.state = {'onset_count': 0}

	def step(self, row):
		'''
		step is run for each row of an aggregate
		- update the state variable based on the row information
		- the input is a mapping between field labels and row data
		'''
		self.state['onset_count'] += 1
		self.state['moment_id'] = row['moment_id']

	def finalize(self):
		'''
		finalize is run at the end of each aggregate
		- return a list of fields corresponding to the 'newFields' and
		  'outputFields' established in the run function
		'''
		self.output.append([('moment_id', self.state['moment_id']),
							('onsetcount', self.state['onset_count'])])
	
	def sql(self):
		part = self.part()
		note = part.add_note()
		note.select_alias('row_id', 'note_id')

		moment = note.start_moment()
		moment.select_alias('row_id', 'moment_id')

if __name__ == '__main__':
	query = Query()
	query.run(printing=True)
