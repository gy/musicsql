#!/usr/bin/python

import string
import musicsql

class Query(musicsql.Aggregate):

	def table_data(self):
		self.requires = ()
		self.foreignkey = ('moment_id', 'moments')
		self.groupFields = ['moment_id']
		self.field_types['quality'] = self.types['string']
		self.field_types['root'] = self.types['integer']
		self.field_types['inversion'] = self.types['string']
		q = {'0': ['unison', 0],
			 '0 1': ['minor second', 0],
			 '0 2': ['major second', 0],
			 '0 3': ['minor third', 0],
			 '0 4': ['major third', 0],
			 '0 5': ['fifth', 0],
			 '0 6': ['tritone', 0], # not necessarily true
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

	def init(self):
		self.state = {'list': {}}

	def step(self, row):
		semit = row['semit']
		self.state['list'][semit % 12] = 1
		self.state['moment_id'] = row['moment_id']

	def finalize(self):
		s = self.state['list'].keys()
		if not s:
			return
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

		# Find qualities
		result = self.qualities.get(nf, None)
		if result is None:
			return
		type, root_idx = result
		root = (pcs[root_idx] + zero_transpose) % 12
		bass_idx = 0
		for idx in range(len(pcs)):
			if pcs[idx] == s[0]:
				bass_idx = idx
		inv_idx = (bass_idx - root_idx) % len(pcs)
		inversions = ['root', 'first', 'second', 'third', 'na']
		self.output.append([('moment_id', self.state['moment_id']),
							('quality', result[0]),
							('root', root),
							('inversion', inversions[inv_idx])])

	def sql(self):
		part = self.part()
		note = part.add_note()
		note.select('semit')

		simul = note.add_simultaneity()
		simul.select_alias('row_id', 'moment_id')

if __name__ == '__main__':
	query = Query()
	query.run(printing=True)
