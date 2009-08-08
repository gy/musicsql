#!/usr/bin/python

'''
A package for running queries on a MusicSQL database.
'''
import sys


def list_submodules():
	import os.path
	import os
	import musicsql.database
	dir = os.path.dirname(musicsql.database.__file__)
	modules = {}
	for file in os.listdir(dir):
		if not file.endswith('.py'): continue
		if not file.startswith('add_'): continue
		base = file[:len(file) - 3]
		modules[base[4:]] = base
	return modules

def load_submodule(name):
	import imp
	import os.path
	import musicsql.database
	modules = list_submodules()
	file = modules.get(name)
	if not file:
		sys.exit("The '%s' property is not available.\n" % name)
	dir = os.path.dirname(musicsql.database.__file__)
	info = imp.find_module(file, [dir])
	module = imp.load_module('musicsql.database.' + file, *info)
	return module

def table_in_database(table, **options):
	import musicsql.backend
	tables = musicsql.backend.list_tables(**options)
	if table in tables:
		return True
	return False
