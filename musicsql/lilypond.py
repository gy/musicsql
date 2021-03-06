#!/usr/bin/python

import sys

warn = sys.stderr.write


def lilypond_path(executable):
	import musicsql.options
	
	if (sys.platform == 'darwin'):
		path = 'mac_path'
	elif (sys.platform.startswith('linux')):
		path = 'linux_path'
	elif (sys.platform.startswith('mingw') or sys.platform.startswith('win')):
		path = 'win_path'
		if (executable == 'lilypond'):
			executable = 'lilypond.exe'
		if (executable == 'musicxml2ly'):
			executable = 'musicxml2ly.py'
	else:
		sys.exit("Error: Unknown system platform '%s'." % sys.platform)
	conf_parser = musicsql.options.get_config_parser()
	return conf_parser.get("LilyPond", path) + '/' + executable

def check_lilypond():
	import os.path
	import glob
	import subprocess
	import musicsql
	lilypond = lilypond_path("lilypond")
	try:
		lilypond = os.path.normpath(glob.glob(lilypond)[0])
	except IndexError:
		sys.exit('MusicSQL: Unable to locate lilypond executable.' +
				'You may need to install LilyPond or edit the MusicSQL CONFIG file.')
	p = subprocess.Popen([lilypond, '-v'], stdin=None, stdout=subprocess.PIPE)
	version = p.stdout.readline()
	p.stdout.close()
	version = [x for x in version.split() if x[0].isdigit()][0]
	if musicsql.version(version) < [2, 12]:
		sys.exit('MusicSQL: LilyPond version 2.12 or greater is required.')
	return lilypond

def make_previews(result_handle, headers, **options):
	import os.path
	import tempfile
	import shutil
	import musicsql.lilypond
	import musicsql.database.exportxml
	
	warn('Making previews...\n')
	tmp_dir = tempfile.mkdtemp()
	rows = []
	for row in result_handle:
		columns = row.rstrip().split('\t')
		hash = dict(zip(headers, columns))
		rows.append(hash)
	lyfile_list = []
	out_files = []
	format = options.get('format', 'png')
	for row in rows:
		xmlfile = musicsql.database.exportxml.previewdataToXml(row, tmp_dir, **options)
		lyfile = musicsql.lilypond.xml_to_ly(xmlfile, tmp_dir)
		lyfile_list.append(lyfile)
		file = os.path.splitext(os.path.basename(row['_file']))[0]
		out_files.append("%s_m%s_%s.%s" % (file, row['_measures'], row['_parts'], format))
	prev_files = musicsql.lilypond.ly_to_preview(lyfile_list, tmp_dir, format)
	for i in range(len(prev_files)):
		shutil.copy(prev_files[i], out_files[i])
	try:
		shutil.rmtree(tmp_dir)
	except:
		pass
	return prev_files

def xml_to_ly(filename, tmp_dir):
	import subprocess
	import tempfile
	import os.path
	import glob
	
	check_lilypond()
	mxml2ly = lilypond_path("musicxml2ly")
	try:
		mxml2ly = os.path.normpath(glob.glob(mxml2ly)[0])
	except IndexError:
		sys.exit('MusicSQL: Unable to locate musicxml2ly. ' +
				'You may need to install LilyPond or edit the MusicSQL CONFIG file.')
	fileinfo = tempfile.mkstemp(dir=tmp_dir)
	m2ly_file = fileinfo[1]
	warn('Converting to LilyPond format...\n')
	errs = tempfile.TemporaryFile()
	shell = False
	if (sys.platform.startswith('mingw') or sys.platform.startswith('win')):
		shell = True
	p = subprocess.Popen([mxml2ly, '-o', m2ly_file, filename], stderr=errs, shell=shell)
	p.wait()
	if p.returncode > 0:
		errs.seek(0)
		sys.exit("musicxml2ly error: %s" % errs.read())
	try:
		m2ly_handle = file(m2ly_file + '.ly', 'r+')
	except IOError:
		warn("Error: Problem reading LilyPond file.\n" +
		     "  Is this a malformed MusicXML file?\n")
		return None
	ly_data = m2ly_handle.read()
	
	m2ly_handle.seek(0)
	m2ly_handle.write('#(set-default-paper-size "a4")\n')# +
#		              '\header { tagline = ##f }\n')
	m2ly_handle.write(ly_data)
	m2ly_handle.close()
	return m2ly_file

def ly_to_preview(filename_list, tmp_dir, format='png'):
	import os
	import tempfile
	import subprocess
	import musicsql

	warn('Running LilyPond...\n')	
	lilypond = check_lilypond()
	
	cwd = os.getcwd()
	os.chdir(tmp_dir)
	err1 = tempfile.TemporaryFile()
	err2 = tempfile.TemporaryFile()
	p = subprocess.Popen([lilypond, '--' + format, '-dpreview'] + filename_list, stdout=err1, stderr=err2)
	p.wait()
	if p.returncode > 0:
		err2.seek(0)
		sys.exit("lilypond error: %s" % err2.read())
	os.chdir(cwd)
	return ["%s.preview.%s" % (x, format) for x in filename_list]
