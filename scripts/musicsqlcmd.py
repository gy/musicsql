#!/usr/bin/python

import sys
import pdb
import getopt
from musicsql.options import getOptions, defaults as longOptions

warn = sys.stderr.write

def print_template():
	print '''#!/usr/bin/python

import musicsql
q = musicsql.Query()

### CONSTRUCT YOUR QUERY HERE ###
part1 = q.part()
note1 = part1.add_first_note()

### END OF QUERY ###

q.print_run()
# use the --previewdata option to get information to generate previews'''

def list_databases():
	import musicsql.backend
	usage = 'usage: musicsqlcmd.py list databases [--password] [--backend dbtype]\n'
	options, args = getOptions(['backend=?', 'password?', 'host=?', 'user=?'], usage)
	dbs = musicsql.backend.list_databases(**options)
	print "Available databases:"
	for db in dbs:
		print "  " + db

def list_modules():
	import musicsql.database as msql
	print "\n:: Modules can be invoked with the 'add' function to add additional properties to nodes."
	options, args = getOptions()
	
	modules = msql.list_submodules().keys()
	modules.sort()
	hubs = {}
	for property in modules:
		module = msql.load_submodule(property)
		instance = module.Query(**options)
		instance.table_data()
		key = instance.foreignkey[1]
		if not hubs.has_key(key):
			hubs[key] = []
		hubs[key].append(property)
	for hub, modules in hubs.items():
		print "\nAvailable modules for '%s':" % hub
		modules.sort()
		for module in modules:
			print '  %s' % module

def list_properties():
	usage = '''
 usage: musicsqlcmd.py list properties node --database database [--password] [--backend dbtype]
 
 :: Properties are the data you can select, test, and compare. 
 :: Note that some some nodes are simply true/false tests and don't have properties.
 '''
	options, args = getOptions(longOptions, usage)
	if len(args) != 1:
		sys.exit(usage)
	import musicsql.backend
	table = args[0]
	cols = musicsql.backend.list_columns(table, **options)
	cols = [x for x in cols if not x.startswith('_')]
	if len(cols) == 0:
		sys.exit("There are no accessible properties in the '%s' table. " % table +
				"This probably means the node is true/false and can be accessed via the is_null method.")
	print "Properties of '%s':" % table
	for col in cols:
		print '  ' + col

def list_hub_nodes():
	usage = ('usage: musicsqlcmd.py list nodes --database database [--password] [--backend dbtype]\n')
	options, args = getOptions(longOptions, usage)
	warn('Introspecting database...\n')
	import musicsql.alchemy as alchemy
	q = alchemy.SQL(**options)
	for type in ('notes', 'noteheads', 'parts', 'moments', 'events'):
		print("\nAdditional nodes for the '%s' objects:" % type)
		nodes = q.relatives[type].keys()
		nodes.sort()
		if 'intersects' in nodes:
			nodes.remove('intersects')
		for table in nodes:
			print '\t' + table

def list_files():
	usage = 'usage: musicsqlcmd.py list files --database database [--password] [--backend dbtype]\n'
	options, args = getOptions(longOptions, usage)
	import musicsql.backend
	db = musicsql.backend.connect(**options)
	sql = 'SELECT path FROM files;'
	cursor = db.cursor()
	cursor.execute(sql)
	files = [x[0] for x in cursor.fetchall()]
	print "Files available in the database:"
	for file in files:
		print "  " + file

def list_values():
	usage = ('usage: musicsqlcmd.py list values node property ' +
			 '--database database [--password] [--backend dbtype]\n')
	options, args = getOptions(longOptions, usage)
	if len(args) != 2:
		sys.exit(usage)
	import musicsql.backend
	db = musicsql.backend.connect(**options)
	cursor = db.cursor()
	args.reverse()
	args = tuple(args)
	cursor.execute('SELECT DISTINCT %s FROM %s;' % args)
	values = [str(x[0]) for x in cursor.fetchall()]
	values.sort()
	print "Values for the '%s' property in the '%s' node:" % args
	for value in values:
		print '  ' + value

def setup_database():
	usage = ('usage: musicsqlcmd.py setup --database database [--password] [--backend dbtype]\n')
	import musicsql.database.setup as setup
	options, args = getOptions(longOptions, usage)
	try:
		setup.setup(**options)
	except KeyboardInterrupt:
		sys.exit(1)

def import_file():
	usage = 'usage: musicsqlcmd.py import file --database database [--password] [--backend dbtype]\n'
	import musicsql.database.importxml as importxml
	options, args = getOptions(longOptions, usage)
	if not (args):
		sys.exit(usage)
	try:
		importxml.importxml(*args, **options)
	except KeyboardInterrupt:
		sys.exit(1)

def delete():
	usage = 'usage: musicsqlcmd.py delete (database|file) ...\n'
	if len(sys.argv) < 2:
		warn(usage)
		sys.exit(2)
	type = sys.argv.pop(1)
	if type == 'database':
		usage = 'usage: musicsqlcmd.py delete database --database name [--password] [--backend dbtype]\n'
		options, args = getOptions(longOptions, usage)
		import musicsql.backend
		musicsql.backend.drop_database(**options)
	elif type == 'file':
		warn("File deletion: coming soon!\n")
	else:
		warn(usage)
		sys.exit(2)

def kill():
	usage = '''
usage: musicsqlcmd.py kill pid [--password] [--backend dbtype]

:: kills a database process
'''
	options, args = getOptions(['backend=?', 'password?'], usage)
	if not args:
		sys.exit(usage)
	import musicsql.backend
	pid = int(args[0])
	musicsql.backend.kill_process(pid, **options)

def add_feature():
	usage = '''
usage: musicsqlcmd.py add module --database database [--password] [--backend dbtype]

::  adds a new node property to the database using a module.
'''
	options, args = getOptions(longOptions, usage)
	if not args:
		sys.exit(usage)
	import musicsql.database as msql
	module = msql.load_submodule(args[0])
	options['tablename'] = args[0]
	query = module.Query(**options)
	try:
		query.run()
	except KeyboardInterrupt:
		pass

def export_xml():
	usage = ('usage: musicsqlcmd.py export [--measures n-m | --ticks n-m] ' +
			 '[--parts part1,part2...] [--highlighted noteid1,noteid2...] ' +
			 'file --database database [--password] [--backend dbtype]\n')
	l_options = longOptions[:]
	l_options += ['measures=?', 'ticks=?', 'parts=?', 'highlighted=?']
	options, args = getOptions(l_options, usage)
	mm, tt = options.get('measures'), options.get('ticks')
	if (not args) or (mm and tt):
		sys.exit(usage)
	import musicsql.database.exportxml
	options['path'] = args[0]
	musicsql.database.exportxml.export(**options)

def dump_stdin(tmp_dir):
	import tempfile
	from os import fdopen
	warn("Reading from standard input:\n")
	try:
		data = sys.stdin.read()
	except KeyboardInterrupt:
		sys.exit(1)
	if not data:
		sys.exit(0)
	fileinfo = tempfile.mkstemp(dir=tmp_dir)
	tmphandle = fdopen(fileinfo[0], 'w')
	tmphandle.write(data)
	tmphandle.close()
	return fileinfo[1]

def previewxml():
	import tempfile
	import musicsql.lilypond
	
	tmp_dir = tempfile.mkdtemp()
	usage = '''
 usage: musicsqlcmd.py previewxml [--format (pdf|png)] [--prefix prefix] [file file2 ...]
 
 :: A convenience function to create a preview of a MusicXML file using LilyPond.
 '''
	longOptions = ('format=?', 'prefix=?')
	options, args = getOptions(longOptions, usage)
	format = options.get('format', 'png')
	prefix = options.get('prefix', 'tmp')
	if not args:
		tmpfile = dump_stdin(tmp_dir)
		args = (tmpfile,)
	ly_files = []
	for filename in args:
		ly_file = musicsql.lilypond.xml_to_ly(filename, tmp_dir)
		ly_files.append(ly_file)
	musicsql.lilypond.ly_to_preview(ly_files, tmp_dir, format)	
	import shutil
	shutil.rmtree(tmp_dir)

def preview():
	import musicsql.lilypond
	usage = '''
usage: musicsqlcmd.py preview file1 [file2...] [--format (pdf|png)] [--output_prefix prefix] --database db_name [--password] [--backend dbtype]
 
:: Reads search results generated with the --previewdata option and creates preview images from them using LilyPond.)
'''
	l_options= longOptions[:]
	l_options += ['format=?', 'prefix=?']
	options, args = getOptions(longOptions, usage)
	import tempfile
	tmp_dir = tempfile.mkdtemp()
	if not args:
		tmpfile = dump_stdin(tmp_dir)
		args = (tmpfile,)
	index = 1
	for file in args:
		filelist = musicsql.lilypond.make_previews(file, tmp_dir, index, **options)
		index += len(filelist)


def ezsearch():
	usage = '''
usage: musicsqlcmd.py ezsearch [--preview] 'searchstring' --database database [--password] [--backend dbtype]

:: A convenience function to search for a simple note sequence using Humdrum rhythm and scientific pitch notation.
:: e.g., '4G3 8A3 8B3 4C4'. Either rhythm or pitch information can be omitted.
'''
	longOpts = longOptions + ['preview?', 'format=?']
	options, args = getOptions(longOpts, usage)
	if len(args) < 1:
		sys.exit(usage)
	options['previewdata'] = True
	options['printing'] = True

	import re
	import musicsql.alchemy
	import musicsql.database
	warn('Constructing query...\n')
	q = musicsql.Query(**options)
	part = q.part()

	details = re.split(' ', args[0])
	note1 = part.add_first_note(details.pop(0))
	notes = note1.add_note_sequence(*details)
	
	result_handle, headers = q.run()
#	if 'preview' in options:
#		musicsql.lilypond.make_previews(result_handle, headers, **options)
		

def main():
	usage = ('usage: musicsqlcmd.py (setup|import|ezsearch|export|list|' +
			 'add|delete|preview|previewxml|kill|template) ...\n')
	if len(sys.argv) < 2:
		warn(usage)
		sys.exit(2)
	function = sys.argv.pop(1)

	if function == 'setup':
		setup_database()
	elif function == 'import':
		import_file()
	elif function == 'delete':
		delete()
	elif function == 'ezsearch':
		ezsearch()
	elif function == 'list':
		usage = '''
usage: musicsqlcmd.py list (databases|files|modules|properties|nodes|values)

:: Note: the list type argument must immediately follow the 'list' keyword.
'''
		if len(sys.argv) < 2:
			warn(usage)
			sys.exit(2)
		type = sys.argv.pop(1)
		if type == 'modules':
			list_modules()
		elif type == 'properties':
			list_properties()
		elif type == 'files':
			list_files()
		elif type == 'nodes':
			list_hub_nodes()
		elif type == 'values':
			list_values()
		elif type == 'databases':
			list_databases()
		else:
			warn(usage)
			sys.exit(2)
	elif function == 'add':
		add_feature()
	elif function == 'export':
		export_xml()
	elif function == 'preview':
		preview()
	elif function == 'previewxml':
		previewxml()
	elif function == 'kill':
		kill()
	elif function == 'template':
		print_template()
	else:
		warn(usage)
		sys.exit(2)

try:
	main()
except KeyboardInterrupt:
	sys.exit('Exiting.')
