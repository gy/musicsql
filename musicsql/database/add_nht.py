#!/usr/bin/python

import string
import musicsql

class Query(musicsql.Aggregate):

	def table_data(self):
		self.requires = ['quality']
		self.groupFields = ['moment_id']
		self.field_types['note_id'] = self.types['integer']
		self.field_types['quality_wo_nht'] = self.types['text']
		self.foreignkey = ('moment_id', 'moments')
		q = {'0': ['unison', 0],
			 '0 1': ['minor second', 0],
			 '0 2': ['major second', 0],
			 '0 3': ['minor third', 0],
			 '0 4': ['major third', 0],
			 '0 5': ['fifth', 0],
			 '0 6': ['tritone', None],
			 '0 3 6': ['diminished', 0],
			 '0 3 7': ['minor', 0],
			 '0 4 7': ['major', 0],
			 '0 4 8': ['augmented', 0],
			 '0 2 6': ['dominant (no fifth)', 1],
			 '0 2 5': ['minor-minor (no fifth)', 1],
			 '0 1 4': ['minor-major (no fifth)', 1],
			 '0 1 5': ['major-major (no fifth)', 1],
			 '0 2 5 8': ['half-diminished', 1],
			 '0 3 6 9': ['fully-diminished', 0],
			 '0 3 5 8': ['minor-minor', 2],
			 '0 1 4 8': ['minor-major', 1],
			 '0 3 6 8': ['dominant', 3],
			 '0 1 5 8': ['major-major', 1],
			 '0 3 4 8': ['augmented-7th', 2],
			 '0 2 4 8': ['dominant +5', 2],
			 '0 2 6 8': ['dominant d5', 3],
			 '0 2 4 6': ['dominant 9 (no fifth)', 1],
			 '0 2 3 6': ['dominant -9 (no fifth)', 1],
			 '0 2 4 6 9': ['dominant 9', 1],
			 '0 2 3 6 9': ['dominant -9', 1]}
		self.qualities = q

	def find_quality(self, s):
		if not s:
			return None
		s.sort()
		# Find the normal form
		s.append(s[0]+12)
		i = [(s[x+1] - s[x], x) for x in range(len(s)-1)]
		i2 = [ [(i+i)[x+y] for y in range(len(i))]
			   for x in range(0, len(i)) ]
		i.sort()
		i3 = [x for x in i2 if x[-1][0] == i[-1][0]]
		i3.sort()
		zero_transpose = s[i3[0][0][1]]
		pcs = [0]
		p = 0
		for n in range(len(i) - 1):
			p += i3[0][n][0]
			pcs.append(p)
		nf = ' '.join([str(x) for x in pcs])
		result = self.qualities.get(nf, None)
		if result is None:
			return None
		return result[0]

	def init(self):
		self.state = {'list': []}

	def step(self, row):
		info = [row['duration'], row['semit'], row['note_id']]
		self.state['list'].append(info)
		self.state['moment_id'] = row['moment_id']

	def finalize(self):
		notes = self.state['list']
		opts = []
		for i in range(len(notes)):
			chord = {}
			for x in range(len(notes)):
				if x != i:
					chord[notes[x][1] % 12] = 1
			quality = self.find_quality(chord.keys())
			if quality:
				unlikely = 1
				if (quality == 'major' or quality == 'minor' or
					quality == 'diminished' or
					quality == 'dominant' or
					quality == 'half-diminished' or
					quality == 'fully-diminished'):
					unlikely = 0
				elif (quality == 'minor third' or
					  quality == 'major third' or
					  quality == 'fifth'):
					unlikely = 0.5
				opt = [unlikely, notes[i][0], 84 - notes[i][1],
					   notes[i][2], quality]
				opts.append(opt)
		if opts:
			opts.sort()
			self.output.append([('moment_id', self.state['moment_id']),
								('note_id', opts[0][3]),
								('quality_wo_nht', opts[0][4])])

	def sql(self):
		part = self.part()
		note = part.add_note()
		note.select('semit', 'duration')
		note.select_alias('row_id', 'note_id')

		simul = note.add_simultaneity()
		quality = simul.node('quality') 
		simul.add_constraint(quality.is_null())
		simul.select_alias('row_id', 'moment_id')

if __name__ == '__main__':
	query = Query()
	query.run(printing=True)
