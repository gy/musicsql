#!/usr/bin/python

import musicsql

class Query(musicsql.Aggregate):

	def table_data(self):
		self.requires = ()
		self.foreignkey['moment_id'] = 'moments'
		self.foreignkey['note_id'] = 'notes'
			# add a foreign key to connect the new table to one of the
			# main hub tables (moments, notes, parts, noteheads)
		self.groupFields = ['moment_id']
			# list of field names used to group aggregates 

	def init(self):
		'''
		init is run at the start of each new aggregate
		- set up the state variable
		'''
		self.state = {'list': []}

	def step(self, row):
		'''
		step is run for each row of an aggregate
		- update the state variable based on the row information
		- the input is a mapping between field labels and row data
		'''
		data = [int(row['semit']), row['part_id'], row['note_id']]
		self.state['list'].append(data)
		self.state['moment_id'] = row['moment_id']

	def finalize(self):
		'''
		finalize is run at the end of each aggregate
		- return a list of fields corresponding to the 'newFields' and
		  'outputFields' established in the run function
		'''
		notes = self.state['list']
		notes.sort()
		self.output.append([('moment_id', self.state['moment_id']),
							('note_id', notes[0][-1])])

	def sql(self):
		part = self.part()
		part.select_alias('row_id', 'part_id')

		note = part.add_first_note()
		note.select('semit')
		note.select_alias('row_id', 'note_id')

		simul = note.add_simultaneity()
		simul.select_alias('row_id', 'moment_id')

if __name__ == '__main__':
	query = Query()
	query.run(printing=True)
