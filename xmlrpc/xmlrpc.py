
from pylons.controllers import XMLRPCController
from tg import session
import sqlalchemy.ext.serializer
import musicsql
import musicsql.backend

class XMLRPCServiceController(XMLRPCController):
	host = 'localhost'
	user = 'root'
	password = ''
	backend = 'mysql'
	stdargs = {'host': host, 'user': user, 'password': password,
			   'backend': backend}

	def _load(self):
		return sqlalchemy.ext.serializer.loads(session['query'])
	
	def _save(self, query):
		session['query'] = sqlalchemy.ext.serializer.dumps(query)
		session.save()

	def list_databases(self):
		"""listdatabases: returns a list of available search databases.
		"""
		dbs = musicsql.backend.list_databases(**self.stdargs)
		return '\n'.join(dbs)
	list_databases.signature = [['struct']]

	def connect(self, database, **args):
		"""connect: start a MusicSQL query connected to the named database.
		"""
		args = self.stdargs.copy()
		args['database'] = database
		session['options'] = args
		query = musicsql.Query(**args)
		self._save(query)
		return True
	connect.signature = [['struct', 'string']]

	def list_hubs(self):
		pass
	
	def list_nodes(self):
		pass

	def list_files(self):
		return str(session)

	def list_modules(self):
		pass

	def list_values(self):
		pass

	def add_property(self, node, property):
		pass

	def export(self, file, measures=None, parts=None):
		pass

	def preview(self, file, measures=None, parts=None):
		pass

	def list_sql_functions(self):
		pass

	def run_query(self):
		query = self._load()
		query.run()
		import os
		return os.getcwd()

	def part(self):
		"""part(): creates a new Part and returns a reference to it.
		"""
		query = self._load()
		part = query.part()
		self._save(query)
		return part.name
	part.signature = [['struct']]

	def hub_node(self, hub, node):
		pass

	def conditional(self):
		pass

	def moment_method(self, node, method, *args):
		"""moment_function(moment, function, arg): executes the Moment's method with the given arg.
		"""
		query = self._load()
		moment = query.get_node(node)
		if method == 'add_note':
			node = args.pop(0)
			part = query.get_node(node)
			details = args.pop(0)
			note = moment.add_note(part, details)
			self._save(query)
			return note.name
		else:
			sys.exit("Note function '%s' is not defined." % function)
	moment_method.signature = [['struct', 'string', 'string', 'string'], 
							['struct', 'string', 'string', 'string', 'string']]

	def note_method(self, node, method, *args):
		"""note_function(note, function, arg): executes the Note's method with the given arg.
		"""
		query = self._load()
		note = query.get_node(node)
		if method == 'start_moment':
			moment = note.start_moment()
			self._save(query)
			return moment.name
		elif method == 'add_previous_note':
			details = args.pop(0)
			note2 = note.add_previous_note(details)
			self._save(query)
			return note2.name
		else:
			sys.exit("Note function '%s' is not defined." % function)
	note_method.signature = [['struct', 'string', 'string'], 
							['struct', 'string', 'string', 'string']]

	def part_method(self, node, method, *args):
		"""part_function(part, function, arg): executes the Part's method with the given arg.
		"""
		query = self._load()
		part = query.get_node(node)
		if method == 'add_first_note':
			details = args.pop(0)
			note = part.add_first_note(details)
			self._save(query)
			return note.name
		else:
			sys.exit("Part function '%s' is not defined." % function)
	part_method.signature = [['struct', 'string', 'string', 'string']]

	def notehead_method(self, node, method, *args):
		pass

	def event_method(self, node, method, *args):
		pass

	def conditional_method(self, node, method, *args):
		pass

	def add_constraint(self, node, expression):
		pass

	def expression(self, object1, relationship, object2):
		pass

	def sql_function(self, function, *args):
		pass

	def select(self, object, property, alias=None):
		pass

	def execute(self):
		pass
