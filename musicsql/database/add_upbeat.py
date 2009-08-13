#!/usr/bin/python

import musicsql

class Query(musicsql.Aggregate):

	def table_data(self):
		self.requires = ()
		
		# list of field names used to group aggregates
		self.groupFields = ['m_id']		
		self.field_types['upbeatmoment_id'] = self.types['integer']

		# add a foreign key to connect the new table to one of the
		# main hub tables (moments, notes, parts, noteheads)
		self.foreignkey['downbeatmoment_id'] = 'moments'

	# init is run at the start of each new aggregate
	# - set up the state variable
	def init(self):
		self.state = {'list': {}}

	# step is run for each row of an aggregate
	# - update the state variable based on the row information
	# - the input is a mapping between field labels and row data
	def step(self, row):
		self.state['list'][row['beat']] = row['priormoment_id']
		self.state['beats'] = row['beats']
		self.state['downbeatmoment_id'] = row['downbeatmoment_id']

	# finalize is run at the end of each aggregate
	# - return a list of fields corresponding to the 'newFields' and
	#   'outputFields' established in the run function
	def finalize(self):
		result = None
		list = self.state['list']
		# Find upbeats at beats
		beat_list = [x for x in list.items() if x[0] == int(x[0])]
		if beat_list:
			beat_list.sort()
			row = [('downbeatmoment_id', self.state['downbeatmoment_id']),
				   ('upbeatmoment_id',beat_list[-1][1])]
			self.output.append(row)
			return
		# Find upbeats at complex beats
		beats = self.state['beats']
		if ((int(beats / 3) == float(beats) / 3) and (beats > 3)):
			upbeat = beats - 3 + 1
			if list.has_key(upbeat):
				row = [('downbeatmoment_id', self.state['downbeatmoment_id']),
					   ('upbeatmoment_id', list[upbeat])]
				self.output.append(row)
				return
		# Find upbeats at non-beats
		beat_list = list.items()
		beat_list.sort()
		if beat_list[-1][0] != int(beat_list[-1][0]):
			row = [('downbeatmoment_id', self.state['downbeatmoment_id']),
				   ('upbeatmoment_id', beat_list[-1][1])]
			self.output.append(row)

	def sql(self):
		part = self.part()
		moment1 = part.add_moment()
		moment1.select_alias('row_id', 'priormoment_id')

		moment2 = moment1.add_later_moment()
		moment2.select_alias('row_id', 'downbeatmoment_id')

		event1 = moment1.event(part)
		measure1 = event1.node('measures')
		event1.select('beat')

		attributes = event1.node('attributes')
		attributes.select('beats')

		event2 = moment2.event(part)
		measure2 = event2.node('measures')
		event2.add_constraint(event2.c.beat == 1)
		event2.add_constraint(measure1.c.number==(measure2.c.number - 1))
		measure2.select_alias('row_id', 'm_id')

if __name__ == '__main__':
	query = Query()
	query.run(printing=True)
