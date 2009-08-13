#!/usr/bin/python

import musicsql

class Query(musicsql.Query):

	def table_data(self):
		self.requires = ()
		self.foreignkey['note_id'] = 'notes'
#		self.field_types['contrarymotion'] = self.types['integer']

	def sql(self):
		part1 = self.part()
		note1 = part1.add_first_note()
		moment = note1.start_moment()
		note1.select_alias('row_id', 'note_id')

		part2 = self.part()
		note2 = part2.add_note(moment=moment)
		pnote1 = note1.add_previous_note()
		pnote2 = note2.add_previous_note()
		n1s = note1.c['semit']
		n2s = note2.c['semit']
		p1s = pnote1.c['semit']
		p2s = pnote2.c['semit']
#		mint1 = note1.c['semit'] - pnote1.c['semit']
#		mint2 = note2.c['semit'] - pnote2.c['semit']
		bothup = self.conditional().IF(n1s > p1s).AND(n2s > p2s)
		bothdown = self.conditional().IF(n1s < p1s).AND(n2s < p2s)
		contrary = self.conditional()
		contrary.IF(n1s == p1s).OR(n2s == p2s).OR(bothup).OR(bothdown)
		note1.add_constraint(contrary)
#		self.select_expression(contrary, 'contrarymotion')

if __name__ == '__main__':
	query = Query()
	query.run(printing=True)
