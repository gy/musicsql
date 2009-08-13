#!/usr/bin/python

import musicsql

class Query(musicsql.Query):

	def table_data(self):
		self.requires = []
		self.foreignkey['note_id'] = 'notes'
		self.field_types['ideal_vl'] = self.types['integer']

	def sql(self):
		part1 = self.part()
		note1 = part1.add_first_note()
		note1.select_alias('row_id', 'note_id')

		part2 = self.part()
		moment = note1.start_moment()
		note2 = part2.add_note(moment=moment)
		note2.select_alias('row_id', 'othernote_id')

		pnote1 = note1.add_previous_note_or_beat()
		pnote2 = note2.add_previous_note_or_beat()
		idealVL = self.conditional()
		idealVL.IF(pnote1.c['row_id'] != note1.c['prevnote_id']).THEN(1)
		idealVL.IF(pnote2.c['row_id'] != note2.c['prevnote_id']).THEN(1)
		idealVL.ELSE(0)
		self.select_expression(idealVL, 'ideal_vl')
		
		mint1 = note1.c['semit'] - pnote1.c['semit']
		mint2 = note2.c['semit'] - pnote2.c['semit']
		hint = (note1.c['semit'] - note2.c['semit']) % 12
		upminor1 = self.conditional().IF (hint == 7).AND (mint1 == 2).AND (mint2 == 3)
		upminor2 = self.conditional().IF (hint == -7).AND (mint1 == 3).AND (mint2 == 2)
		upmajor1 = self.conditional().IF (hint == 7).AND (mint1 == 2).AND (mint2 == 4)
		upmajor2 = self.conditional().IF (hint == -7).AND (mint1 == 4).AND (mint2 == 2)
		downmajor1 = self.conditional().IF (hint == 7).AND (mint1 == -2).AND (mint2 == -5)
		downmajor2 = self.conditional().IF (hint == -7).AND (mint1 == -5).AND (mint2 == -2)
		downminor1 = self.conditional().IF (hint == 7).AND (mint1 == -1).AND (mint2 == -5)
		downminor2 = self.conditional().IF (hint == -7).AND (mint1 == -5).AND (mint2 == -1)
		hornfifth = self.conditional().IF (upminor1).OR(upminor2).OR(upmajor1).OR(
			upmajor2).OR (downmajor1).OR (downmajor2).OR (downminor1).OR (downminor2)
		note1.add_constraint(hornfifth)

if __name__ == '__main__':
	q = Query()
	q.run()
