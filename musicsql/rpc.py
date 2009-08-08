#!/usr/bin/python

import SimpleXMLRPCServer
import musicsql.alchemy

def list_databases():
	pass

def list_files():
	pass

def list_modules():
	pass

def list_values():
	pass

def add_property(node, property):
	pass

def export(file, measures=None, parts=None):
	pass

def preview(file, measures=None, parts=None):
	pass

def list_sql_functions():
	pass

def open_query(database):
	pass

def run_query():
	pass

def part(name=None):
	pass

def hub_node(hub, node):
	pass

def conditional():
	pass

def moment_function(moment, function, *args):
	pass

def note_function(note, function, *args):
	pass

def part_function(part, function, *args):
	pass

def notehead_function(notehead, function, *args):
	pass

def event_function(event, function, *args):
	pass

def conditional_function(conditional, function, *args):
	pass

def add_constraint(node, expression):
	pass

def expression(object1, relationship, object2):
	pass

def sql_function(function, *args):
	pass

def select(object, property, alias=None):
	pass

def execute():
	pass
	
server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', 61427))
server.register_introspection_functions()
	
server.serve_forever()
