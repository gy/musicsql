#!/usr/bin/python

__all__ = ['version']

import re
import sys
import pdb
import sqlalchemy
import musicsql.backend
import musicsql.alchemy

warn = sys.stderr.write

def version(ver):
	import string
	for i in range(len(ver)):
	    if not ver[i] in '0123456789.':
	        break
	if i < len(ver) - 1:
	    ver = ver[:i]
	return map(int, string.split(ver, '.'))

if version(sys.version) < [2,4]:
    sys.exit('MusicSQL: Python version 2.4 or greater is required.\n')
if version(sqlalchemy.__version__) < [0,5]:
    sys.exit('MusicSQL: SQLalchemy 0.5 or greater is required.\n')


class Query:

	def __init__(self, **options):
		import musicsql
		if not options:
			import musicsql.options
			options = musicsql.options.default_options()
		self.options = options.copy()
		self.errors = musicsql.backend.get_errors(self.options['backend'])
		self.alchemy = musicsql.alchemy.SQL(**options)
		self.types = {'integer': sqlalchemy.Integer,
					'string': sqlalchemy.String(32), # just a useful default
					'float': sqlalchemy.Float,
					'text': sqlalchemy.Text}
		self.outputFields = []
		self.field_types = {}
		self.notehead_command = None
		self.requires = []
		self.distinct = False
		self.cursors = {}
		self.check_requirements()

	def sql(self):
		sys.exit("Query error: Either use the Query object to programmatically " +
				"construct a query, or override the 'sql' method.\n")

	def table_data(self):
		sys.exit("Query error: If the 'tablename' option is set, " +
				"you must override the 'table_data' method.\n")

	@staticmethod
	def conditional(IF=None, THEN=None, ELSE=None):
		return musicsql.alchemy.Conditional(IF=IF, THEN=THEN, ELSE=ELSE)
	
	@staticmethod
	def sqlfunction(name):
		import sqlalchemy
		return getattr(sqlalchemy.func, name)

	def part(self):
		return self.alchemy.part()

	def select_expression(self, expression, label):
		self.alchemy.select_expression(expression, label)
	
	def select_all(self):
		self.alchemy.select_all()

	def set_option(self, option, value):
		self.options[option] = value

	def print_run(self):
		self.run(printing=True)

	def run(self, unique=False, message=None, printing=False):
		if not self.alchemy.structures:
			self.sql()
		sql = self.alchemy.assemble_query()
		if 'verbose' in self.options:
			print sql
		if message:
			warn(message + '\n')
		connection = self.alchemy.bind.connect()
#		self.database = connection

		results = self.setup_query(sql, connection)
		filename, headers = self.write_results_to_file(results, connection)
		connection.close()
		results_file = open(filename)
		if 'tablename' in self.options:
			table = self.newTable(headers, unique)
			self.write_to_table(results_file, headers, table)
		if printing or 'printing' in self.options:
			print '\t'.join(headers)
			results_file.seek(0)
			for row in results_file:
				print row.rstrip()
			results_file.seek(0)
		return results_file, headers

	def check_requirements(self):
		if 'tablename' in self.options:
			self.table_data()
		nodes = {}
		for node in self.requires:
			nodes[node] = 0
		todo = nodes.keys()
		module_list = []
		tables = self.alchemy.metadata.tables.keys()
		while todo:
			node = todo[0]
			if node not in tables:
				module_list.append(node)
				module = load_submodule(node)
				instance = module.Query(**self.options)
				for item in instance.requires:
					if not nodes.has_item(item):
						nodes[item] = 0
			nodes[node] = 1
			todo = [x for x, y in nodes.items() if y == 0]
		if module_list:
			warn("Error: The following pre-requisites are missing " +
				 "from the database:\n")
			for module in module_list:
				warn('  ' + module + '\n')
			sys.exit(1)

	def insertRows(self, cursor, sql, rows):
		cursor.executemany(sql, rows)

	def newTable(self, output_headers, unique):
		name = self.options['tablename']
		table = self.alchemy.metadata.tables.get(name) 
		if table:
			table.drop()
			self.alchemy.new_metadata()

		columns = []
		for field in output_headers:
			type = self.alchemy.field_types.get(field, None)
			if not type:
				type = self.field_types.get(field, None)
			if not type:
				sys.exit("Type error: no type set for '%s'." % field)
			col = sqlalchemy.Column(field, type)
			columns.append(col)
		if self.foreignkey:
			col = sqlalchemy.Column(self.foreignkey[0], self.types['integer'], 
								sqlalchemy.ForeignKey("%s.row_id" % self.foreignkey[1]))
			columns.append(col)
		if unique:
			columns.append(sqlalchemy.UniqueConstraint(*columns))
		columns.append(sqlalchemy.Column('row_id', self.types['integer'], primary_key=True))
		table = sqlalchemy.Table(name, self.alchemy.metadata, *columns)
		table.create()
		self.alchemy.new_metadata()
		table = self.alchemy.metadata.tables[name]
		return table

	def setup_query(self, sql, connection):
		backend = self.options['backend']
		self.progress = {'percent': 0,
						 'limit': self.get_progress_limit(sql, connection)}
		if isinstance(self, musicsql.Aggregate):
			select_str = sql[:sql.find('FROM ')]
			fields = select_str.rstrip().split(',')
			cols = [re.split('[ .]', x)[-1] for x in fields]
			idx = [str(cols.index(x) + 1) for x in self.groupFields]
			sql += ' ORDER BY ' + ', '.join(idx)
		from_str = sql[sql.find('FROM'):sql.find('WHERE')]
		join_count = from_str.count(',') + 1
		musicsql.backend.prep_sql(sql, self.options['backend'])
		sql = sql.replace('%', '%%')
#		self.database.commit()
		pid = musicsql.backend.thread_id(connection, backend)
		if pid:
			warn('Executing query (%d joins, thread ID %d)...\n' % (join_count, pid))
		else:
			warn('Executing query (%d joins)...\n' % join_count)
		try:
			results = connection.execute(sql + ';')
		except sqlalchemy.exc.OperationalError, err:
			sys.exit('SQL error %s\n' % err.orig)
		return results

	def summarize_exportables(self, row, headers, connection):
		if '_path' in headers:
			headers.remove('_path')
			for key in headers[:]:
				if key.startswith('_nidx_'):
					headers.remove(key)
			headers += ['_file', '_measures', '_parts', '_noteheads']
		file = row['_path']
		del row['_path']
		notedict = {}
		for key, val in row.items():
			if key.startswith('_nidx_'):
				notedict[key] = val
				del row[key]
		if not self.notehead_command:
			notelist = [str(x) for x in notedict.iterkeys()]
			params = musicsql.backend.parameter_list(notelist, self.options['backend'])
			params = ', '.join(params)
			self.notehead_command = '''
			  SELECT measures.number, parts.part_attr
			  FROM noteheads
			  JOIN events on events.row_id = noteheads.startevent_id
			  JOIN parts on parts.row_id = events.part_id
			  JOIN measures on measures.row_id = events.measure_id
			  WHERE noteheads.row_id IN (%s)
			''' % params
		results = connection.execute(self.notehead_command, notedict)
		results = [list(x) for x in results.fetchall()]
		measures = [x[0] for x in results]
		measures.sort()
		measures = '%d-%d' % (measures[0], measures[-1])
		parts = {}
		for x in results:
			parts[str(x[1])] = 1
		parts = ','.join(parts.keys())
		noteheads = notedict.values()
		noteheads.sort()
		noteheads = ','.join([str(x) for x in noteheads])
		row['_file'] = file
		row['_measures'] = measures
		row['_parts'] = parts
		row['_noteheads'] = noteheads

	def show_progress(self, rows, headers, span=[0, 100]):
		if '_progress' in headers:
			headers.remove('_progress')
 		for row in rows:
			p_val = row.get('_progress', None)
			if not p_val:
				return
			self.progress_report(p_val, *span)

	def progress_report(self, status, min=0, max=100):
		progress = self.progress
		diff = max - min
		percent = int('%d' % (min + diff * status / progress['limit']))
		if percent > max:
			return
		if percent > progress['percent']:
			warn('Progress: %d%%\n' % percent)
			progress['percent'] = percent

	def get_progress_limit(self, sql, connection):
		sources = re.findall('FROM (\w+)', sql)
		c_sql = 'SELECT count(row_id) FROM %s;' % sources[0]
		result = connection.execute(c_sql)
		row = result.fetchone()
		return int(row[0])

	def write_results_to_file(self, results, connection):
		import os
		import tempfile
		warn('Fetching results...\n')
		(tmpDescriptor, tmpfilename) = tempfile.mkstemp()
		resultFile = os.fdopen(tmpDescriptor, 'w')
		progress = self.progress
		progress['result_rows'] = 0
		input_headers = []
		final_output_headers = []
		distinct = {}
		while True:
			rows = results.fetchmany(size=100)
			if not rows:
				break
			if not input_headers:
				input_headers = rows[0].keys()
			output_headers = input_headers[:]
			rows = [dict(x.items()) for x in rows]
			span = (0, 100)
			if 'tablename' in self.options:
				span = (0, 50)
			self.show_progress(rows, output_headers, span)
			if 'exportable' in self.options:
				for row in rows:
					self.summarize_exportables(row, output_headers, connection)
			if isinstance(self, musicsql.Aggregate):
				rows = self.applyAggregate(rows, output_headers)
			elif isinstance(self, musicsql.Function):
				rows = self.applyFunction(rows, output_headers)
			if self.distinct:
				for idx in range(len(rows) - 1, -1, -1):
					data = [str(rows[idx][x]) for x in output_headers]
					hash = '\t'.join(data)
					if distinct.has_key(hash):
						del rows[idx]
					else:
						distinct[hash] = 1
			for row in rows:
				text = '\t'.join([str(row[x]) for x in output_headers])
				resultFile.write(text + '\n')
			progress['result_rows'] += len(rows)
			if not final_output_headers:
				final_output_headers = output_headers
		output_headers = final_output_headers
		results.close()
		if progress['result_rows'] == 0:
			warn('No data returned!\n')
			sys.exit(1)
		resultFile.flush()
		resultFile.close()
		return tmpfilename, output_headers

	def write_to_table(self, file_handle, headers, table):
		warn('Inserting data into database...\n')
		file_handle.seek(0)
		self.progress['percent'] = 0
		self.progress['limit'] = self.progress['result_rows']
		row_count = 0
		data = []
		for row in file_handle:
			row_count += 1
			self.progress_report(row_count, 50, 100)
			fields = row.rstrip().split('\t')
			data.append(dict(zip(headers, fields)))
		self.alchemy.bind.dialect.paramstyle = 'format'
		table.insert().execute(data)
		return

class Function(Query):

	def __init__(self, **options):
		Query.__init__(self, **options)
		self.output = []

	def function(self, row):
		warn('The "function" method must be overridden ' +
			 'in a Function object.\n')
		sys.exit(2)

	def add_output(self, row):
		self.output.append(row)

	def applyFunction(self, rows, headers):
		def fix_headers(input, output, headers):
			missing = [x[0] for x in output if not x[0] in headers]
			if not missing: return
			new_headers = [x[0] for x in output]
			if input.has_key('_noteheads'):
				new_headers += ['_file', '_measures', '_parts',
								'_noteheads']
			del headers[:]
			headers += new_headers

		output = []
		for row in rows:
			self.function(row)
			for outputRow in self.output:
				fix_headers(row, outputRow, headers)
				data = dict(outputRow)
				if row.has_key('_file'):
					data['_file'] = row['_file']
					data['_measures'] = row['_measures']
					data['_parts'] = row['_parts']
					data['_noteheads'] = row['_noteheads']
				output.append(data)
			del self.output[:]
		return output


class Aggregate(Query):

	def __init__(self, **options):
		Query.__init__(self, **options)
		self.prevGroupInfo = None
		self.output = []
		self.noteheads = []

	def init(self):
		warn('The "init" function must be overridden ' +
			 'in an Aggregate object.\n')
		sys.exit(2)

	def step(self, row):
		warn('The "step" function must be overridden ' +
			 'in an Aggregate object.\n')
		sys.exit(2)

	def finalize(self):
		warn('The "finalize" function must be overridden ' +
			 'in an Aggregate object.\n')
		sys.exit(2)

	def add_output(self, row):
		self.output.append(row)

	def applyAggregate(self, rows, headers):
		def finalize(row, output, headers):
			self.finalize()
			self.init()
			if not self.output: return
			for outputRow in self.output:
				fix_headers(row, outputRow, headers)
				data = dict(outputRow)
				if row.has_key('_file'):
					data['_file'] = row['_file']
					data['_measures'] = row['_measures']
					data['_parts'] = row['_parts']
					data['_noteheads'] = row['_noteheads']
				output.append(data)
			del self.output[:]

		def fix_headers(input, output, headers):
			missing = [x[0] for x in output if not x[0] in headers]
			if not missing and len(output) == len(headers):
				return
			new_headers = [x[0] for x in output]
			if input.has_key('_noteheads'):
				new_headers += ['_file', '_measures', '_parts',
								'_noteheads']
			del headers[:]
			headers += new_headers

		output = []
		for row in rows:
			groupInfo = ' '.join([str(row[x])
								  for x in self.groupFields])
			if not self.prevGroupInfo:
				self.init()
			elif groupInfo != self.prevGroupInfo:
				finalize(row, output, headers)
			self.step(row)
			self.prevGroupInfo = groupInfo
			self.oldRow = row
		if (not rows) and self.prevGroupInfo:
			finalize(self.oldRow, output, headers)
		return output
