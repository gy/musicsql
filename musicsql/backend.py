#!/usr/bin/python

import sys
import pdb
import sqlalchemy

warn = sys.stderr.write

def backends():
	return '(mysql|postgres|sqlite)'

def get_errors(backend):
	errors = {}
	if backend == 'sqlite':
		from pysqlite2 import dbapi2 as sqlite
		errors['OperationalError'] = sqlite.OperationalError
		errors['DatabaseError'] = sqlite.DatabaseError
		errors['ProgrammingError'] = sqlite.ProgrammingError
		errors['IntegrityError'] = sqlite.IntegrityError
	elif backend == 'postgres':
		import psycopg2
		errors['OperationalError'] = psycopg2.OperationalError
		errors['DatabaseError'] = psycopg2.DatabaseError
		errors['ProgrammingError'] = psycopg2.ProgrammingError
		errors['IntegrityError'] = psycopg2.IntegrityError
	elif backend == 'mysql':
		import _mysql_exceptions
		errors['OperationalError'] = _mysql_exceptions.OperationalError
		errors['DatabaseError'] = _mysql_exceptions.DatabaseError
		errors['ProgrammingError'] = _mysql_exceptions.ProgrammingError
		errors['IntegrityError'] = _mysql_exceptions.IntegrityError
	else:
		warn("The backend '%s' is not supported.\n" % backend)
		sys.exit(2)
	return errors

def thread_id(connection, backend):
	if backend == 'mysql':
		return connection.connection.thread_id()
	else:
		# not implemented
		return None

def prep_sql(sql, backend):
	if backend == 'sqlite':
		sql = sql.replace('% ', '%% ')
	return sql

def kill_process(pid, **options):
	backend = options['backend']
	db = admin_connection(**options)
	errors = get_errors(backend)
	if backend == 'mysql':
		try:
			db.raw_connection().kill(pid)
		except errors['OperationalError']:
			sys.exit('Error: unable to kill process %d.' % pid)	
	else:
		warn("The backend '%s' is not supported.\n" % backend)
		sys.exit(2)

def alcconnect(**options):
	backend = options['backend']
	if 'database' not in options:
		sys.exit("Error: the 'database' option is required.")
	database = options['database']
	password = options.get('password', '')
	if password:
		password = ':' + password
	host = options.get('host', 'localhost')
	if backend == 'postgres':
		user = options.get('user', 'postgres')
	elif backend == 'mysql':
		user = options.get('user', 'root')
	elif backend == 'sqlite':
		user = ''
	else: 
		sys.exit("The backend '%s' is not supported." % backend)
	url = '%s://%s%s@%s/%s' % (backend, user, password, host, database)
	db = sqlalchemy.create_engine(url)
	if backend == 'mysql':
		try:
			db.execute('SET GLOBAL storage_engine=InnoDB;')
		except sqlalchemy.exc.OperationalError, err:
			sys.exit("SQL error %s" % err.orig)
		db.execute('SET SQL_BIG_SELECTS=1;')
	return db
	
	
def connect(**options):
	backend = options['backend']
	if 'database' not in options:
		sys.exit("Error: the 'database' option is required.")
	database = options['database']
	password = options.get('password', '')
	host = options.get('host', 'localhost')
	errors = get_errors(backend)
	db = None
	if backend == 'sqlite':
		from pysqlite2 import dbapi2 as sqlite
		db = sqlite.connect(database)
	elif backend == 'postgres':
		import psycopg2
		user = options.get('user', 'postgres')
		dsn_str = ("dbname='%s' user='%s' host='%s' password='%s'"
				   % (database, user, host, password))
		try:
			db = psycopg2.connect(dsn=dsn_str)
		except errors['OperationalError'], err:
			sys.exit("Database error: %s\n" % err)
	elif backend == 'mysql':
		import MySQLdb
		opts = {'db': database, 'host': host, 'passwd': password}
		opts['user'] = opts.get('user', 'root')
		try:
			db = MySQLdb.connect(**opts)
		except errors['OperationalError'], err:
			sys.exit("Database error: %s\n" % err)
		cursor = db.cursor()
		cursor.execute('SET AUTOCOMMIT=0;')
		cursor.close()
	else:
		warn("The backend '%s' is not supported.\n" % backend)
		sys.exit(2)
	return db

def activate_geqo(cursor):
	geqo = [('geqo', 'on'), ('geqo_threshold', '12'),
			('geqo_effort', 5), ('geqo_pool_size', 0),
			('geqo_generations', 0), ('geqo_selection_bias', 2.0)]
	for item in geqo:
		cursor.execute('set %s to %s;' % item)

def drop_database(**options):
	backend = options['backend']
	if backend == 'sqlite':
		sys.exit('To delete an SQLite database, just put the file in the Trash.\n')
	password = options.get('password', '')
	if 'database' not in options:
		sys.exit("musicsql.backend.drop_database: the 'database' argument is required.")
	db = admin_connection(**options)
	try:
		db.execute('drop database %s;' % options['database'])
	except sqlalchemy.exc.OperationalError, err:
		sys.exit("SQL error %s" % err.orig)


def list_columns(table, **options):
	db = alcconnect(**options)
	meta = sqlalchemy.MetaData()
	meta.bind = db
	try:
		table = sqlalchemy.Table(table, meta, autoload=True)
	except sqlalchemy.exc.NoSuchTableError, err:
		sys.exit("Error: This database does not have a '%s' node." % err)
	cols = [x.name for x in table.c if not x.name.endswith('_id')]
	return cols

def list_databases(**options):
	backend = options['backend']
	errors = get_errors(backend)
	internal_dbs = ()
	if backend == 'postgres':
		command = 'select datname from pg_database;'
		internal_dbs = ('postgres', 'template0', 'template1')
	elif backend == 'mysql':
		command = 'SHOW DATABASES;'
		internal_dbs = ('information_schema', 'mysql')
	else:
		warn("The backend '%s' is not supported.\n" % backend)
		sys.exit(2)
	database = admin_connection(**options)
	result = database.execute(command)
	dbs = [x[0] for x in result]
	for name in dbs[:]:
		if name in internal_dbs:
			dbs.remove(name)
			continue
		options['database'] = name
		tables = list_tables(**options)
		if 'noteheads' not in tables:
			dbs.remove(name)
	return dbs
		
def type_lookup(database, backend):
	type_lookup = {}
	if backend == 'sqlite':
		pass
	elif backend == 'postgres':
		sql = 'SELECT oid, typname from pg_catalog.pg_type'
		cur = database.cursor()
		cur.execute(sql)
		for row in cur.fetchall():
			type_lookup[row[0]] = row[1]
		cur.close()
	elif backend == 'mysql':
		import MySQLdb.constants.FIELD_TYPE as ft
		for key, val in ft.__dict__.iteritems():
			if key == 'VAR_STRING':
				key = 'TEXT'
			elif not isinstance(val, type(1)):
				continue
			if key == 'LONG':
				key = 'INT'
			type_lookup[val] = key
	else:
		warn("The backend '%s' is not supported.\n" % backend)
		sys.exit(2)
	return type_lookup
	

def table_options(backend):
	pkey = post = ''
	if backend == 'sqlite':
		pkey = 'row_id INTEGER PRIMARY KEY, '
	elif backend == 'postgres':
		pkey = 'row_id SERIAL PRIMARY KEY, '
	elif backend == 'mysql':
		pkey = 'row_id INT AUTO_INCREMENT NOT NULL, ' + \
			   'PRIMARY KEY (row_id), '
		post = ' ENGINE=InnoDB'
	else:
		warn("The backend '%s' is not supported.\n" % backend)
		sys.exit(2)
	return pkey, post


def parameter_list(headers, backend):
	if backend == 'sqlite':
		paramList = [':%s' % x for x in headers]
	elif backend in ('postgres', 'mysql'):
		paramList = ['%%(%s)s' % x for x in headers]
	else:
		warn("The backend '%s' is not supported.\n" % backend)
		sys.exit(2)
	return paramList


def assignment_list(params, backend):
	l = []
	if backend in ('postgres', 'mysql'):
		l = ["%s = %%(%s)s" % (k.replace('-', '_'), k)
			 for k in params]
	elif backend == 'sqlite':
		l = ["%s = :%s" % (k.replace('-', '_'), k) for k in params]
		query = ' AND '.join(l)
	else:
		warn("The '%s' backend is not supported.\n" % backend)
		sys.exit(1)
	return l
	

def create_engine(**options):
	return alcconnect(**options)
	import sqlalchemy
	if 'database' not in options:
		sys.exit("Error: the 'database' option is required.")
	host = options.get('host', 'localhost')
	if options['backend'] == 'sqlite':
		url = 'sqlite:///%s' % options['database']
	elif options['backend'] == 'postgres':
		user = options.get('user', 'postgres')
		if 'password' in options:
			user += ':' + options['password']
		url = 'postgres://%s@%s:5432/%s' % (user, host, options['database'])
	elif options['backend'] == 'mysql':
		user = options.get('user', 'root')
		if 'password' in options:
			user += ':' + options['password']
		url = 'mysql://%s@%s/%s' % (user, host, options['database'])
	else:
		warn("The backend '%s' is not supported.\n" % options['backend'])
		sys.exit(2)
	db = sqlalchemy.create_engine(url)
	if options['backend'] == 'mysql':
		db.execute('SET storage_engine=InnoDB;')
	return db

def list_tables(**options):
	db = alcconnect(**options)
	meta = sqlalchemy.MetaData()
	meta.reflect(bind=db)
	return [x.name for x in meta.sorted_tables]

def admin_connection(**options):
	backend = options['backend']
	password = options.get('password', '')
	if password:
		password = ':' + password
	host = options.get('host', 'localhost')
	if backend == 'postgres':
		user = options.get('user', 'postgres')
		database = '/template1'
	elif backend == 'mysql':
		user = options.get('user', 'root')
		database = ''
	elif backend == 'sqlite':
		sys.exit('Error: SQLite does not support admin connections.\n')
	url = '%s://%s%s@%s%s' % (backend, user, password, host, database)
	db = sqlalchemy.create_engine(url)
	return db

def create_database(**options):
	backend = options['backend']
	if backend == 'sqlite':
		return
	db = admin_connection(**options)
	try:
		db.execute('CREATE DATABASE %s;' % options['database'])
	except sqlalchemy.exc.ProgrammingError, err:
		sys.exit("SQL error %s" % err.orig)
	return

def lastrowid(cursor, table, backend):
	if backend == 'sqlite':
		return cursor.lastrowid
	elif backend == 'postgres':
		sql = "SELECT currval(pg_get_serial_sequence('%s','row_id'));" % table
		cursor.execute(sql)
		return int(cursor.fetchone()[0])
	elif backend == 'mysql':
		return cursor.lastrowid
	else:
		warn("The '%s' backend is not supported.\n" % backend)
		sys.exit(1)
