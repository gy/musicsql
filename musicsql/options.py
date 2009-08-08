#!/usr/bin/python

'''
A module for processing command-line and configuration file options.
'''

__all__ = ['getOptions', 'get_config_parser', 'defaults']
defaults = ['exportable?', 'password?', 'printing?', 'preview?', 'database=', 
				'host=?', 'user=?', 'backend=?', 'verbose?']

import sys

ConfParser = None

def get_config_parser():
	global ConfParser
	if ConfParser:
		return ConfParser	
	import musicsql
	import os.path
	import ConfigParser
	path = os.path.dirname(musicsql.__file__) + '/CONFIG'
	ConfParser = ConfigParser.SafeConfigParser()
	ConfParser.readfp(open(path))
	return ConfParser


def getOptions(longOpts=[], usageError=''):
	'''Process command-line options and check for errors.

	This is basically a wrapper for getopt.getopt that also constructs
	short options and checks for required arguments.

	'longOpts' is expected to be a list of long option names. Names
	can be followed by an optional '=' to indicate a required
	argument, and an optional '?' to indicate the argument is
	optional (in that order).

	'usageError' is an optional usage error string to print if an
	error is raised.
	'''

	import getopt
	warn = sys.stderr.write
	shortOptions = ''
	longOptions = []
	required_options = []
	lookup = {}
	for i in longOpts:
		if i.endswith('?'):
			i = i[:-1]
		else:
			required_options.append(i.strip('='))
		longOptions.append(i)
		longopt = i.strip('=')
		lookup['--' + longopt] = longopt
		opt = i[0]
		if shortOptions == None or shortOptions.find(opt) >= 0:
			continue
		lookup['-' + opt] = longopt
		if i.endswith('='):
			opt += ':'
		shortOptions += opt
	try:
		option_list, args = getopt.gnu_getopt(sys.argv[1:], shortOptions,
											  longOptions)
	except getopt.GetoptError, (errno, errstr):
		sys.exit(usageError + errno + '\n')
	options = {}
	for key, val in option_list:
		options[lookup[key]] = val
	for key in required_options:
		if not options.get(key):
			warn(usageError + 
				 "Required '%s' argument is missing.\n" % key)
			sys.exit(2)
	if 'backend' not in options:
		options['backend'] = 'mysql'
	if 'password' in options:
		import getpass
		options['password'] = getpass.getpass(stream=sys.stderr)
	return options, args

def default_options(**presets):
	global defaults
	import string
	import os.path
	caller = os.path.basename(sys.argv[0])
	opts = defaults[:]
	for idx in range(len(opts)):		
		newopt = "--%s" % opts[idx]
		if newopt.endswith('?'):
			newopt = '[%s]' % newopt[:-1]
		loc = newopt.find('=')
		if loc > 0:
			newopt = "%s arg%s" % (newopt[:loc], newopt[loc+1:])
		opts[idx] = newopt
	argstr = string.join(opts, ' ')
	usage = ('usage: %s %s\n' % (caller, argstr))
	options, args = getOptions(defaults, usage)
	for key, value in presets.items():
		options[key] = value
	return options
