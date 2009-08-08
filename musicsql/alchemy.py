#!/usr/bin/python


__all__ = ['SQL', 'Part', 'Moment', 'Event', 'Simultaneity',
		   'Note', 'Notehead']

import re
import sys
import pdb
import string
import musicsql.backend
import sqlalchemy
import sqlalchemy.sql.expression

warn = sys.stderr.write

class SQL(sqlalchemy.sql.expression.Select):
	State = None
	
	def __init__(self, **options):
		if 'database' not in options:
			sys.exit("Error: SQL.setup requires a 'database' argument.\n")
		self.bind = musicsql.backend.create_engine(**options)
		sqlalchemy.sql.expression.Select.__init__(self, [], bind=self.bind)
		SQL.State = self
		self.new_metadata()
		self.options = options
		self.structures = []
		self.data = []
		self.tables = {}
		self.relatives = {}
		self.column_objs = []
		self.column_list = []
		self.alias_names = []
		self.field_types = {}
		self._introspect()

	def new_metadata(self):
		try:
			self.metadata = sqlalchemy.MetaData(self.bind, reflect=True)
		except sqlalchemy.exc.OperationalError, err:
			sys.exit("Error: unable to connect to database. %s" % err.orig) 
	
	def _introspect(self):
		def _set_foreignkey_count(tables, table, column):
			c = tables[table].c[column]
			result = sqlalchemy.select([c], distinct=True).execute()
			count = 0
			while result.fetchone():
				count += 1
			tables[table].foreigncount[column] = count
		def _find_dependent_tables(t2, metadata):	
			found = []
			result = []
			for name, t in metadata.tables.iteritems():
				for c in t.c:
					for foreignkey in c.foreign_keys:
						if foreignkey.column.table == t2:
							if t not in found:
								found.append(t)
								result.append([t2.name, t.name, c.name])
			return result
	
		table_list = self.metadata.tables.keys()
		for idx in table_list:
			tbl = sqlalchemy.Table(idx, self.metadata, autoload=True)
			self.tables[idx] = tbl
			sql_count = sqlalchemy.func.count
			result = sqlalchemy.select([sql_count(tbl.c['row_id'])]).execute()
			tbl.count = int(result.fetchone()[0])
			tbl.foreigncount = {}
		hub_list = ('notes', 'noteheads', 'parts', 'moments', 'events')
		for idx in hub_list:
			self.relatives[idx] = {}
			deps = _find_dependent_tables(self.tables[idx], self.metadata)
			for item in deps:
				if item[1] in hub_list:
					continue
				self.relatives[idx][item[1]] = item
				_set_foreignkey_count(self.tables, item[1], item[2])
			for c in self.tables[idx].c:
				for foreignkey in c.foreign_keys:
					parent = foreignkey.column.table.name
					if parent in hub_list:
						continue
					rel = [parent, idx, c.name]
					self.relatives[idx][parent] = rel
					_set_foreignkey_count(self.tables, idx, c.name)
	
	# adapted from code contributed to SQLALchemy wiki
	# by brettatoms@gmail.com
	
	def _find_tables(self, clause, dict):
		kind = str(type(clause))
		if isinstance(clause, sqlalchemy.Column):
			dict[clause.table] = 1
			clause.index = True
		elif (string.find(kind, 'Calculated') >= 0
			  or string.find(kind, 'Compound') >= 0
			  or string.find(kind, 'ClauseList') >= 0):
			for c in clause.clauses:
				self._find_tables(c, dict)
		elif string.find(kind, 'BindParam') >= 0:
			shortname = clause.shortname
			if shortname == 'literal':
				return
			col = [x for x in self.column_objs if x.name == shortname]
			if col:
				for bound in col[0]._get_orig_set():
					self._find_tables(bound, dict)
		elif string.find(kind, 'Expression') >= 0:
			self._find_tables(clause.left, dict)
			self._find_tables(clause.right, dict)
		elif string.find(kind, 'Grouping') >= 0:
			self._find_tables(clause.element, dict)
	
	def part(self):
		part = Part()
		exportable = self.options.get('exportable', False)
		if exportable and not self.column_list:
			file = part.node('files')
			file.select_alias('path', '_path')
		return part
		
	def assemble_query(self, order_by=None, group_by=[], having=None,
					   distinct=False, progress=True):
		self.metadata.bind.dialect.paramstyle = 'pyformat'
		s = sqlalchemy.select(distinct=distinct)
		s.order_by(order_by)
		s.group_by(*group_by)
		if having:
			s.append_having(having)
	
		skip_list = {}
		consDict = {}
		for node in self.data + self.structures:
			for subnode_constraints in node.constraints.values():
				for constraint in subnode_constraints:
					subnodes = {}
					self._find_tables(constraint, subnodes)
					for subnode in subnodes.keys():
						skip_list[subnode] = skip_list.get(subnode, 0) + 1
						consDict[subnode] = consDict.get(subnode, []) + \
											[constraint]
			columns = len(node.select_columns)
			skip_list[node] = skip_list.get(node, 0) + columns
		for node in self.structures:
			for join in node.joins:
				skip_list[join[0]] = skip_list.get(join[0], 0) + 1
				skip_list[node] = skip_list.get(node, 0) + 1
		skip_list = [x for x in skip_list.keys() if skip_list[x] < 2]
		structure_list = [x for x in self.structures if x not in skip_list]
		data_list = [x for x in self.data if x not in skip_list]
	
		where_list = []
		column_list = []
		for object in structure_list + data_list:
			object.add_select_to_query(where_list, column_list)
			s.append_from(object.assemble_join())
		for skip in skip_list:
			for constraint in consDict.get(skip, []):
				for idx in range(len(where_list) - 1, -1, -1):
					if id(constraint) == id(where_list[idx]):
						where_list.pop(idx)
		parts = [x for x in structure_list if isinstance(x, Part)]
		for idx in range(len(parts) - 1):
			node1 = parts[idx]
			node2 = parts[idx + 1]
			constraint = node1.c.file_id == node2.c.file_id
			where_list.append(constraint)
		s.append_whereclause(sqlalchemy.and_(*where_list))
		if progress:
			s.append_column(structure_list[0].c['row_id'].label('_progress'))
		column_dict = {}
		for column in column_list + self.column_objs:
			column_dict[column.name] = column
		for column in self.column_list:
			s.append_column(column_dict[column])
	
		compiler = s.compile()
		params = compiler.construct_params()
		for key, value in params.items():
			if isinstance(key, type(None)):
				key = 'None'
				params[key] = value
			if not isinstance(value, (type(1), type(1.0))):
				params[key] = "'%s'" % value
		sql = str(s)
		sql = musicsql.backend.prep_sql(sql, self.options['backend'])
		sql = sql % params
		return sql
	
	def select_expression(self, column, label):
		if isinstance(column, Conditional):
			column = column.case()
		column = column.label(label)
		self.column_objs.append(column)
		self.column_list.append(column.name)
		self._find_tables(column, {}) # adds indices to all columns
	
	def select_all(self):
		for node in self.data + self.structures:
			node.select_all()


class Conditional():
	
	def __init__(self, IF=None, THEN=None, ELSE=None):
		self.cases = []
		self.elseval = None
		self.ifstate = -1
		if IF:
			self.IF(IF, THEN)
		if ELSE:
			self.ELSE(ELSE)

	def __str__(self):
		return "Conditional(%s)" % ', '.join([str(x) for x in self.cases])
		
	def IF(self, predicate, THEN=None):
		if (isinstance(predicate, Conditional)):
			predicate = predicate.expression()
		idx = len(self.cases)
		clause = [predicate, THEN]
		self.cases.insert(idx, clause)
		self.ifstate = idx
		return self
		
	def ELSEIF(self, predicate, THEN=None):
		self.IF(predicate, THEN)
		return self
	
	def THEN(self, consequent):
		self.cases[self.ifstate][1] = consequent
		return self
		
	def AND(self, predicate):
		if (isinstance(predicate, Conditional)):
			predicate = predicate.expression()
		self.cases[self.ifstate][0] = sqlalchemy.and_(self.cases[self.ifstate][0], predicate)
		return self
				
	def OR(self, predicate):
		if (isinstance(predicate, Conditional)):
			predicate = predicate.expression()
		self.cases[self.ifstate][0] = sqlalchemy.or_(self.cases[self.ifstate][0], predicate)
		return self
				
	def ELSE(self, val):
		self.elseval = str(val)
		return self
	
	def expression(self):
		if (len(self.cases) > 1):
			warn("Warning: You are combining Conditional objects with more than one IF case; " +
				"only the first case is kept.")
		if (self.cases[0][1]):
			warn("Warning: You are combining Conditional objects which have THEN clauses; " +
				"those clauses will be discarded.")
		return self.cases[0][0]
	
	def case(self):
		return sqlalchemy.case(self.cases, else_=self.elseval)


class Node(sqlalchemy.sql.expression.Alias):
	
	def __init__(self, table_name):
		self.SQL = SQL.State
		name = self.alias_name()
		table_name = self.SQL.tables[table_name]
		sqlalchemy.sql.expression.Alias.__init__(self, table_name, alias=name)
		self.constraints = {}
		self.select_columns = []
		self.joins = []

	def alias_name(self):
		import random
		while True:
			name = "%s%06d" % (self.__class__.__name__, random.randint(0, 999999))
			if name not in self.SQL.alias_names:
				break
		self.SQL.alias_names.append(name)
		return name

	def assemble_join(self):
		base = self
		for join in self.joins:
			if join[2] == 'natural':
				base = base.join(join[0], join[1])
			elif join[2] == 'outer':
				base = base.outerjoin(join[0], join[1])
			else:
				warn("Error: the '%s' join type is not supported.\n" % join[2])
				sys.exit(1)
		return base

	def add_select_to_query(self, where_list, column_list):
		for object, conditions in self.constraints.iteritems():
			for condition in conditions:
				where_list.append(condition)
		for column in self.select_columns:
			column_list.append(column)

	def select(self, *columns):
		for column_name in columns:
			self.SQL.column_list.append(column_name)
			column = self.c[column_name]
			self.select_columns.append(column)
			column.index = True
			self.SQL.field_types[column_name] = column.type

	def select_alias(self, column, alias):
		self.SQL.column_list.append(alias)
		column = self.c[column].label(alias)
		self.select_columns.append(column)
		column.index = True
		self.SQL.field_types[alias] = column.type
		
	def select_all(self):
		self.select(self.c)

	def list_columns(self):
		columns = []
		for c in self.c:
			if (c.name == 'row_id' or
				string.find(c.name, '_') == 0 or
				string.find(c.name, '_id') == len(c.name) - 3):
				continue
			columns.append(c.name)
		return columns
	
	def is_null(self):
		if not self.foreign_keys:
			warn("Error: 'isNull' can not be called on Node %s " % self.original.name +
				"because it is not dependent on another table (it has no foreign keys).")
			sys.exit(1)
		if len(self.foreign_keys) > 1:
			warn("Error: 'isNull' can not be called on Node %s " % self.original.name +
				"because it is connected to multiple Nodes (is has multiple foreign keys).")
			sys.exit(1)
		foreignkey = tuple(self.foreign_keys)[0]
		return foreignkey.parent != None
		
class Hub(Node):

	def __init__(self, table_name):
		Node.__init__(self, table_name)
		self.relatives = {}

	def list_relatives(self):
		type = self.original.name
		return self.SQL.relatives[type].keys()
	
	def add_constraint(self, constraint, relative=None):
		def find_relative(self, constraint):
			tables = {}
			self.SQL._find_tables(constraint, tables)
			constraint_node = None
			for table in tables.keys():
				if (table in self.relatives.values()):
					return table
					break
			if not constraint_node:
				if (self in tables.keys()):
					return self
				else:
					warn("Error: Hub.add_constraint() requires that the " + 
						 "constraint include the Hub or a Node " + 
						 "attached to it.\n")
					sys.exit(2)
		
		if (isinstance(constraint, Conditional)):
			constraint = constraint.expression()
		if not relative:
			relative = find_relative(self, constraint)
		if not self.constraints.has_key(relative):
			self.constraints[relative] = []
		self.constraints[relative].append(constraint)

	def node(self, table_name, join=None):
		type = self.original.name
		if table_name in self.relatives:
			return self.relatives[table_name]
		elif table_name in self.SQL.relatives[type]:
			table = Node(table_name)
			lookup = {self.original.name: self,
					  table_name: table}
			relate = self.SQL.relatives[type][table_name]
			col1 = lookup[relate[0]].c['row_id']
			col2 = lookup[relate[1]].c[relate[2]]
			col1.index = True
			col2.index = True
			if not join:
				if (self.SQL.tables[relate[0]].count != 
					self.SQL.tables[relate[1]].foreigncount[relate[2]]):
					join = 'outer'
				else:
					join = 'natural'
			self.joins.append([table, col1 == col2, join])
			self.relatives[table_name] = table
			self.SQL.data.append(table)
		else:
			warn(("The property node '%s' is not attached to the " + 
				  "'%s' hub in the current database.\n")
				 % (table_name, self.original.name))
			sys.exit(2)
		return self.relatives[table_name]


class Notehead(Hub):

	def __init__(self, note, event=None, tied_previous=None):
		Hub.__init__(self, 'noteheads')
		self.SQL.data.append(self)
		self.note = note
		self.add_constraint(self.c['note_id'] == note.c['row_id'])
		self.tied_previous = None
		if not event:
			part = note.start_event.part
			moment = Moment(part)
			event = Event(part, moment)
		self.add_constraint(self.c['startevent_id'] == event.c['row_id'])
		self.start_event = event
		if tied_previous:
			self.tied_previous = tied_previous
			c = self.c['tiedto_id'] == tied_previous.c['row_id']
			self.add_constraint(c)
		if self.SQL.options.get('exportable', False):
			self.select_alias('row_id', '_nidx_' + self.name)

	def add_next_tied_notehead(self):
		last_tied_note = self
		while last_tied_note.tied_next:
			last_tied_note = last_tied_note.tied_next
		notehead = Notehead(self.note, last_tied_note)
		last_tied_note.tied_next = notehead
		return notehead

	def start_slur(self, notehead):
		slur = Node('slurs')
		c = slur.c['startnotehead_id'] == self.c['row_id']
		self.add_constraint(c)
		c = slute.c['stopnotehead_id'] == notehead.c['row_id']
		notehead.add_constraint(c)
		self.add_constraint(self.start_event.c['ticks'] < 
							notehead.start_event.c['ticks'])


class Note(Hub):

	def __init__(self, event, specifier=None):
		Hub.__init__(self, 'notes')
		self.SQL.data.append(self)
		self.start_event = event
#		self.simul_events = []
		self.add_constraint(self.c['startevent_id'] == event.c['row_id'],
							event)
		self.onset_notehead = None
		if self.SQL.options.get('exportable', False):
			self.notehead()
		if specifier:
			self.set_note_details(specifier)

	def notehead(self):
		if not self.onset_notehead:
			self.onset_notehead = Notehead(self, self.start_event)
		return self.onset_notehead

	def part(self):
		return self.start_event.part

	def start_moment(self):
		return self.start_event.moment

	def set_note_details(self, details):
		patt = '(\d+\.*)?([a-gA-G][-#]*\d*)?'
		(recip, pitch) = re.findall(patt, details)[0]
		if pitch:
			self.set_pitch(pitch)
		if recip:
			self.set_recip(recip)
			
	def set_pitch(self, pitch):
		patt = '([a-gA-G])([-#]*)(\d*)'
		(step, alter, octave) = re.findall(patt, pitch)[0]
		step = (ord(step.upper()) - 67) % 7
		self.add_constraint(self.c['concert_step'] == step)
		if alter:
			if alter[0] == '#':
				alter = len(alter)
			else:
				alter = -len(alter)
		else:
			alter = None
		self.add_constraint(self.c['concert_alter'] == alter)
		if octave:
			self.add_constraint(self.c['concert_octave'] == octave)

	def set_recip(self, recip):
		(recip, dots) = re.findall('(\d+)(\.*)', recip)[0]
		divisions = self.start_event.get_divisions()
		if dots:
			denom = 2 ** len(dots)
			num = 0
			for dot in range(len(dots)):
				num += 2 ** dot
			c = self.c['duration'] == divisions * 4 * num / (denom * recip)
		else:
			c = self.c['duration'] == divisions * 4 / recip
		self.add_constraint(c)

	def set_diatonic_interval(self, note2, interval, dir):
		if dir == 'next':
			step = note2.c['concert_step'] - self.c['concert_step']
			oct = note2.c['concert_octave'] - self.c['concert_octave']
		if dir == 'prev':
			step = self.c['concert_step'] - note2.c['concert_step']
			oct = self.c['concert_octave'] - note2.c['concert_octave']
		self.add_constraint(step + oct * 7 == interval)

	def set_chromatic_interval(self, note2, interval, dir):
		if dir == 'next':
			int = note2.c['semit'] - self.c['semit']
		if dir == 'prev':
			int = self.c['semit'] - note2.c['semit']
		self.add_constraint(int == interval)

	def set_contour(self, note2, contour, dir):
		if contour not in ('^', 'v'):
			warn("Contour must be specified as '/' or '\'.\n")
			sys.exit(1)
		if ((dir == 'next' and contour == '^') or
			(dir == 'prev' and contour == 'v')):
			c = note2.c['semit'] > self.c['semit']
		else:
			c = note2.c['semit'] < self.c['semit']
		self.set_constraint(c)

	def add_simultaneity(self):
		s = Simultaneity()
		s.attach_note(self)
		return s

	def detach_startevent(self):
		del self.constraints[self.start_event]

	def process_intervals(self, note, **kargs):
		diatonic = kargs.get('diatonic_interval')
		dir = kargs.get('dir')
		if diatonic:
			self.set_diatonic_interval(note, diatonic, dir)
		chromatic = kargs.get('chromatic_interval')
		if chromatic:
			self.set_chromatic_interval(note, chromatic, dir)

	def add_chordnote(self, details=None, **kargs):
		note = Note(self.event, details)
		self.constraints[note] = [self.c['row_id'] != note.c['row_id']]
		self.process_intervals(note, **kargs)
		return note

	def newevent_note(self, details, **kargs):
		event = self.start_event.part.event(Moment())
		note = Note(event, details)
		self.process_intervals(note, **kargs)
		return note

	def add_previous_note(self, details=None, **kargs):
		note = self.newevent_note(details, dir='prev', **kargs)
		self.constraints[note] = [self.c['prevnote_id'] == note.c['row_id']]
		return note
			
	def add_next_note(self, details=None, **kargs):
		note = self.newevent_note(details, dir='next', **kargs)
		self.constraints[note] = [note.c.prevnote_id == self.c.row_id]
		return note
		
	def add_previous_beat(self, details=None, **kargs):
		note = self.newevent_note(details, dir='prev', **kargs)
		self.constraints[note] = [self.c.prevbeat_id == note.c.row_id]
		return note

	def add_next_beat(self, details=None, **kargs):
		note = self.newevent_note(details, dir='next', **kargs)
		self.constraints[note] = [note.c.prevbeat_id == self.c.row_id]
		return note

	def add_previous_semit(self, details=None, contour=None, **kargs):
		note = self.newevent_note(details, dir='prev', **kargs)
		self.constraints[note] = [self.c.prevsemit_id == note.c.row_id]
		if contour:
			self.set_contour(note, contour, 'prev')
		return note

	def add_next_semit(self, details=None, contour=None, **kargs):
		note = self.newevent_note(details, dir='next', **kargs)
		self.constraints[note] = [note.c.prevsemit_id == self.c.row_id]
		if contour:
			self.set_contour(note, contour, 'next')
		return note

	def add_previous_note_or_beat(self, details=None, **kargs):
		note = self.newevent_note(details, dir='prev', **kargs)
		self.constraints[note] = [sqlalchemy.or_(self.c.prevnote_id == note.c.row_id,
									  			self.c.prevbeat_id == note.c.row_id)]
		return note

	def add_next_note_or_beat(self, details=None, **kargs):
		note = self.newevent_note(details, dir='next', **kargs)
		self.constraints[note] = [sqlalchemy.or_(note.c.prevnote_id == self.c.row_id,
									  			note.c.prevbeat_id == self.c.row_id)]
		return note

	def add_note_sequence(self, *args):
		notelist = []
		last_note = self
		for details in args:
			new_note = last_note.add_next_note(details)
			notelist.append(new_note)
			last_note = new_note
		return notelist

	def add_diatonic_interval_sequence(*args):
		notelist = []
		last_note = self
		for int in args:
			new_note = last_note.add_next_note(diatonic_interval=int)
			notelist.append(new_note)
			last_note = new_note
		return notelist

	def add_contour_sequence(*args):
		notelist = []
		last_note = self
		for dir in args:
			new_note = last_note.add_next_note(contour=int)
			notelist.append(new_note)
			last_note = new_note
		return notelist


class Event(Hub):

	def __init__(self, part, moment):
		Hub.__init__(self, 'events')
		self.SQL.structures.append(self)
		self.add_constraint(self.c['part_id'] == part.c['row_id'])
		self.add_constraint(self.c['moment_id'] == moment.c['row_id'])
		self.part = part
		self.moment = moment
		part.events[moment] = self
		moment.events[part] = self

	def get_divisions(self):
		files = self.node('files')
		return files.c['divisions']
							 
	def start_wedge(self, event):
		wedge = Node('wedges')
		c = slur.c['startevent_id'] == self.c['row_id']
		self.add_constraint(c)
		c = slute.c['stopevent_id'] == event.c['row_id']
		event.add_constraint(c)
		c = self.c['ticks'] < event.c['ticks']
		self.add_constraint(c)

	def part(self):
		return self.part

	def moment(self):
		return self.moment


class Moment(Hub):

	def __init__(self):
		Hub.__init__(self, 'moments')
		self.SQL.structures.append(self)
		self.events = {}

	def add_note(self, part, specifier=None):
		event = Event(part, self)
		note = Note(event, specifier)
		return note

	def add_later_moment(self):
		moment = Moment()
		self.add_constraint(moment.c.ticks > self.c.ticks)
		return moment

	def add_earlier_moment(self):
		moment = Moment()
		self.add_constraint(moment.c.ticks < self.c.ticks)
		return moment

	def event(self, part):
		event = self.events.get(part)
		if not event:
			event = Event(part, self)
		return event

	def event_node(self, node_type, part):
		if not part:
			parts = [x for x in self.SQL.structures
					 if x.original.name == 'parts']
			part = parts[0]
		event = self.event(part)
		return event.node(node_type)

	def measure(self, part=None):
		return self.event_node('measures', part)

	def attributes(self, part=None):
		return self.event_node('attributes', part)


class Part(Hub):

	def __init__(self):
		Hub.__init__(self, 'parts')
		parts = [x for x in self.SQL.structures
				 if x.original.name == 'parts']
		for part in parts:
			c = self.c['row_id'] != part.c['row_id']
			self.add_constraint(c)
		self.SQL.structures.append(self)
		self.events = {}

	def add_note(self, specifier=None, moment=None):
		if not moment:
			for hub in self.SQL.structures:
				if hub.original.name == 'moments':
					warn("Part.add_note(): you must provide a " + 
						 "'moment=' argument if a Moment or Note " + 
						 "already exists.\n") 
					sys.exit(2)
			moment = Moment()
		event = Event(self, moment)
		note = Note(event, specifier)
		return note

	def add_moment(self):
		for hub in self.SQL.structures:
			if hub.original.name == 'moments':
				warn('Part.add_moment() can only be used' + 
					 'if no other moments already exist.\n')
				sys.exit(2)
		moment = Moment()
		return moment

	def event(self, moment):
		event = self.events.get(moment)
		if not event:
			event = Event(self, moment)
		return event


class Simultaneity(Moment):

	def __init__(self):
		Moment.__init__(self)
		self.constraints = {}
		self.intersects = []
		self.events = {}

	def attach_note(self, note):
		i = Node('intersects')
		self.SQL.data.append(i)
		self.intersects.append(i)
		self.add_constraint(i.c['moment_id'] == self.c['row_id'])
		note.add_constraint(i.c['note_id'] == note.c['row_id'])

	def add_note(self, part, specifier=None):
		moment = Moment()
		event = Event(part, moment)
		note = Note(event, specifier)
		i = Node('intersects')
		self.intersects.append(i)
		self.SQL.data.append(i)
		note.add_constraint(i.c['note_id'] == note.c['row_id'])
		self.add_constraint(i.c['moment_id'] == self.c['row_id'])
		return note
