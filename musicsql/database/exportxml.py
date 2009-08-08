#!/usr/bin/python

# To DO:
#  Fix slur handling (slurs must be joined and attached to notes)

import sys
import re
import time
import getopt
import string
import StringIO
import xml.dom.minidom
import musicsql.backend as sqlbackend
import musicsql.dtuple as dtuple

warn = sys.stderr.write
StartTime = 0

def fileHeader(dom, fileInfo, parts, cursor):
	doc = dom.createDocumentFragment()
#	pNode = addElement(dom, doc, 'part-list')
	work = addElement(dom, doc, 'work')
	nodes = [work]
	addElement(dom, work, 'work-number', text=fileInfo['work_number'])
	addElement(dom, work, 'work-title', text=fileInfo['work_title'])
	nodes.append(addElement(dom, doc, 'movement-number',
							text=fileInfo['movement_number']))
	nodes.append(addElement(dom, doc, 'movement-title',
							text=fileInfo['movement_title']))
	partList = addElement(dom, doc, 'part-list')
	nodes.append(partList)
	for part in parts:
		scorePart = addElement(dom, partList, "score-part",
							   attr={'id': part['part_attr']})
		addElement(dom, scorePart, 'part-name', text=part['part_name'])
		addElement(dom, scorePart, 'group', text=part['part_group'])
		addElement(dom, scorePart, 'source', text=fileInfo['source'])
		id = addElement(dom, scorePart, 'identification')
		addElement(dom, id, 'rights', text=fileInfo['rights'])
		encoding = addElement(dom, id, 'encoding')
		addElement(dom, encoding, 'encoding-date',
				   text=fileInfo['encoding_date'])
		addElement(dom, encoding, 'encoder', text=fileInfo['encoder'])
	output = ''
	for node in nodes:
		output += nodeXML(node, '\t')
	return output

def getAttributeBefore(tick, part, buffer, cursor):
	attribute_command = '''
	  SELECT DISTINCT attributes.*, moments.ticks
	  FROM attributes
	  JOIN events ON events.row_id = attributes.startevent_id
	  JOIN moments ON moments.row_id = events.moment_id
	  WHERE events.part_id = %s AND moments.ticks < %d
	  ORDER BY moments.ticks
	''' % (part['row_id'], tick)
	cursor.execute(attribute_command)
	results = cursor.fetchall()
	if not results: return
	desc = dtuple.TupleDescriptor([[f][0] for f
								   in cursor.description])
	buffer.append(dtuple.DatabaseTuple(desc, results[-1]))

def measure_ticks(basicCursor, file_id, measure_range):
	command = '''
	SELECT DISTINCT measures.*
	FROM measures
	WHERE measures.file_id = %s
	ORDER BY measures.number
	''' % file_id
	basicCursor.execute(command)
	measures = fetchrows(basicCursor)
	for n in range(len(measures)):
		measure = measures[n] = dict(measures[n])
		if n < len(measures) - 1:
			measure['end_tick'] = measures[n+1]['_start_tick']
		else:
			measure['end_tick'] = (2 * measure['_start_tick'] -
								   measures[n-1]['_start_tick'])
	measure_min = measures[0]['number']
	measure_max = measures[-1]['number']
	if measure_range:
		measure_range = [int(x) for x in measure_range.split('-')]
	else:
		measure_range = [measure_min, measure_max]
	measure_range[1] += 1
	measures = [x for x in measures
				if x['number'] in range(*measure_range)]
	ticks = (measures[0]['_start_tick'], measures[-1]['end_tick'])
	return ticks, measures

def addParts(dom, fileInfo, parts, db, basicCursor,
			 backend, measure_range, highlighted, printer):
	# Get measures
	ticks, measures = measure_ticks(basicCursor, fileInfo['row_id'], measure_range)
	if highlighted:
		highlighted = highlighted.split(',')
	part_id = sqlbackend.parameter_list(['part_id'], backend)[0]
	# Get voices
	voice_command = '''
	  SELECT DISTINCT voice
	  FROM noteheads
	  JOIN events ON events.row_id = noteheads.startevent_id
	  WHERE events.part_id = %s
	  ORDER BY voice
	''' % part_id
	# Setup measure content cursors
	note_command = '''
	  SELECT noteheads.row_id, noteheads.*, moments.ticks, beam1, beam2, beam3, beam4, beam5, beam6
	  FROM noteheads
	  JOIN events on events.row_id = noteheads.startevent_id
	  JOIN moments on moments.row_id = events.moment_id
	  LEFT OUTER JOIN beams on beams.notehead_id = noteheads.row_id
	  WHERE events.part_id = %s
	  AND moments.ticks >= %d AND moments.ticks < %d 
	  ORDER BY moments.ticks
	''' % (part_id, ticks[0], ticks[1])
	attribute_command = '''
	  SELECT DISTINCT attributes.*, moments.ticks
	  FROM attributes
	  JOIN events ON events.row_id = attributes.startevent_id
	  JOIN moments ON moments.row_id = events.moment_id
	  WHERE events.part_id = %s
	  AND moments.ticks >= %d AND moments.ticks < %d 
	  ORDER BY moments.ticks
	''' % (part_id, ticks[0], ticks[1])
	barline_command = '''
	  SELECT barlines.*, moments.ticks
	  FROM barlines
	  JOIN events ON events.row_id = barlines.event_id
	  JOIN moments ON moments.row_id = events.moment_id
	  WHERE events.part_id = %s 
	  AND moments.ticks >= %d AND moments.ticks < %d 
	  ORDER BY moments.ticks
	''' % (part_id, ticks[0], ticks[1])
	direction_command = '''
	  SELECT directions.*, moments.ticks
	  FROM directions
	  JOIN events ON events.row_id = directions.event_id
	  JOIN moments ON moments.row_id = events.moment_id
	  WHERE events.part_id = %s
	  AND moments.ticks >= %d AND moments.ticks < %d 
	  ORDER BY moments.ticks
	''' % (part_id, ticks[0], ticks[1])
	oldProgress = 0
	for j in range(len(parts)):
		part = parts[j]
		printer.write('\t<part id="%s">\n' % part['part_attr'])
		part_id = {'part_id': part['row_id']}
		basicCursor.execute(voice_command, part_id)
		voices = basicCursor.fetchall()
		voices = [v[0] for v in voices]
		cursors = {'notes': db.cursor(), 'attributes': db.cursor(),
				   'barlines': db.cursor(), 'directions': db.cursor()}
		for cursor in cursors.values():
			cursor.arraysize = 100
		cursors['notes'].execute(note_command, part_id)
		cursors['attributes'].execute(attribute_command, part_id)
		cursors['barlines'].execute(barline_command, part_id)
		cursors['directions'].execute(direction_command, part_id)
		buffers = {'notes': [], 'attributes': [],
				   'barlines': [], 'directions': []}
		fetchmany(buffers['attributes'], cursors['attributes'])
		if not buffers['attributes']:
			getAttributeBefore(ticks[0], part, buffers['attributes'],
							   basicCursor)
		for i in range(len(measures)):
			measure = measures[i]
			doc = dom.createDocumentFragment()
			mNode = addElement(dom, doc, 'measure',
							   attr={'number': measure['number']})
			addMeasureContents(dom, mNode, voices, measure, fileInfo,
							   cursors, buffers, highlighted)
			partInc = 1 / len(parts)
			progress = int(100*(float(j) + float(i) / len(measures)) /
						   len(parts))
			if progress > oldProgress:
				timeLeft = (time.time() - StartTime) * \
						   (100 / float(progress) - 1)
				timeLeft = time.strftime('%M:%S',
										 time.localtime(timeLeft))
				warn('%d%% (%s remaining)\n' % (progress, timeLeft))
				oldProgress = progress
			printer.write(nodeXML(mNode, '\t\t'))
		printer.write('\t</part>\n')

def fetchmany(buffer, cursor):
	if not cursor.description:
		return
	desc = dtuple.TupleDescriptor([[f][0] for f
								   in cursor.description])
	oldLen = -1
	while len(buffer) < 10 and len(buffer) > oldLen:
		oldLen = len(buffer)
		buffer.extend(cursor.fetchmany())
	for i in range(len(buffer)):
		buffer[i] = dtuple.DatabaseTuple(desc, buffer[i])
	
def addMeasureContents(dom, mNode, voices, measureInfo, fileInfo,
						cursors, buffers, highlighted):
	noteList = [[] for x in voices]
	attributeList = []
	barlineList = []
	directionList = []
	measureStart = currentTick = measureInfo['_start_tick']
	nextMeasureStart = measureInfo['end_tick']
	while True:
		if not buffers['notes']:
			fetchmany(buffers['notes'], cursors['notes'])
		if not buffers['notes'] or \
			   buffers['notes'][0]['ticks'] >= nextMeasureStart:
			break
		voice = buffers['notes'][0]['voice']
		noteList[int(voice)-1].append(buffers['notes'].pop(0))
	while True:
		if not buffers['attributes']:
			fetchmany(buffers['attributes'], cursors['attributes'])
		if not buffers['attributes'] or \
			   buffers['attributes'][0]['ticks'] >= nextMeasureStart:
			break
		attributeList.append(buffers['attributes'].pop(0))
	while True:
		if not buffers['barlines']:
			fetchmany(buffers['barlines'], cursors['barlines'])
		if not buffers['barlines'] or \
			   buffers['barlines'][0]['ticks'] >= nextMeasureStart:
			break
		barlineList.append(buffers['barlines'].pop(0))
	while True:
		if not buffers['directions']:
			fetchmany(buffers['directions'], cursors['directions'])
		if not buffers['directions'] or \
			   buffers['directions'][0]['ticks'] >= nextMeasureStart:
			break
		directionList.append(buffers['directions'].pop(0))
	list = attributeList + directionList + noteList[0] + barlineList
	tmplist = [(x['ticks'], dict(x)) for x in list]
	list = [x[1] for x in tmplist]
	for i in range(len(list)):
		if list[i].has_key('voice'):
			chord = isChord(i, list)
			currentTick = adjustTiming(dom, mNode, currentTick,
									   list[i]['ticks'],
									   list[i]['notehead_duration'], chord)
			addNote(dom, mNode, list[i], chord, highlighted)
		elif list[i].has_key('fifths'):
			addAttributes(dom, mNode, list[i], fileInfo)
		elif list[i].has_key('style'):
			addBarline(dom, mNode, list[i])
		elif list[i].has_key('value'):
			addDirection(dom, mNode, list[i])
	for j in range(1, len(voices)):
		list = noteList[j]
		if not list: continue
		for i in range(len(list)):
			chord = isChord(i, list)
			currentTick = adjustTiming(dom, mNode, currentTick,
									   list[i]['ticks'],
									   list[i]['notehead_duration'], chord)
			addNote(dom, mNode, list[i], chord, highlighted)
	adjustTiming(dom, mNode, currentTick, nextMeasureStart)


def adjustTiming(dom, node, currentTick, startTick, duration=0,
				 chord=False):
	diff = currentTick - startTick
	if diff > 0 and not chord:
		backup = addElement(dom, node, 'backup')
		addElement(dom, backup, 'duration', text=diff)
	elif diff < 0:
		forward = addElement(dom, node, 'forward')
		addElement(dom, forward, 'duration', text=-diff)
	currentTick = startTick + int(duration)
	return currentTick
	

def isChord(i, items):
	chord = False
	if i > 0:
		(prev, this) = items[i-1:i+1]
		if prev.has_key('voice') and prev['notehead_duration'] \
			   and this['ticks'] == prev['ticks'] and not prev['grace_order']:
			chord = True
	return chord


def addNote(dom, mNode, note, chord, highlighted):
	noteNode = addElement(dom, mNode, 'note')
	noteNode.attributes['id'] = str(note['row_id'])
	if highlighted:
		if str(note['row_id']) in highlighted:
			noteNode.attributes['color'] = '#FF0000'
	if chord:
		addElement(dom, noteNode, 'chord')
	if note['grace_order']:
		if note['steal_time']:
			if note['steal_time'] > 0:
				att ={'steal-time-following': note['steal_time']}
			else:
				att ={'steal-time-following': -note['steal_time']}
			addElement(dom, noteNode, 'grace', attr=att)
		else:
			addElement(dom, noteNode, 'grace')
	if note['step'] != None:
		pitch = addElement(dom, noteNode, 'pitch')
		addElement(dom, pitch, 'step', text=note['step'])
		if note['alter_']:
			addElement(dom, pitch, 'alter', text=note['alter_'])
		addElement(dom, pitch, 'octave', text=note['octave'])
	if note['rest']:
		addElement(dom, noteNode, 'rest')
	if note['notehead_duration']:
		addElement(dom, noteNode, 'duration', text=note['notehead_duration'])
	addElement(dom, noteNode, 'voice', text=note['voice'])
	if note['staff']:
		addElement(dom, noteNode, 'staff', text=note['staff'])
	if note['tie_stop']:
		addElement(dom, noteNode, 'tie', attr={'type': 'stop'})
	if note['tie_start']:
		addElement(dom, noteNode, 'tie', attr={'type': 'start'})
	if note['type']:
		addElement(dom, noteNode, 'type', text=note['type'])
	if note['dot']:
		for j in range(note['dot']):
			addElement(dom, noteNode, 'dot')
	if note['accidental']:
		addElement(dom, noteNode, 'accidental',
				   text=note['accidental'])
	if note['stem']:
		addElement(dom, noteNode, 'stem', text=note['stem'])
	for idx in range(1, 7):
		beam = 'beam' + str(idx)
		if note[beam]:
			addElement(dom, noteNode, 'beam', attr={'number': idx}, text=note[beam])
	if note['articulation'] or note['dynamics']:
		notations = addElement(dom, noteNode, 'notations')
		if note['articulation']:
			articulation = addElement(dom, notations, 'articulations')
			if string.find(note['articulation'], 'other:') == 0:
				addElement(dom, articulation, 'other-articulation',
						   text=note['articulation'][6:])
			else:
				addElement(dom, articulation, note['articulation'])
		if note['dynamics']:
			dynamics = addElement(dom, notations, 'dynamics')
			if string.find(note['dynamics'], 'other:') == 0:
				addElement(dom, dynamics, 'other-dynamics',
						   text=note['dynamics'][6:])
			else:
				addElement(dom, dynamics, note['dynamics'])
		


def addAttributes(dom, mNode, attributes, fileInfo):
	attNode = addElement(dom, mNode, 'attributes')
	addElement(dom, attNode, 'divisions',
			   text=fileInfo['divisions'])
	if attributes['fifths'] != None:
		key = addElement(dom, attNode, 'key')
		addElement(dom, key, 'fifths', attributes['fifths'])
		if attributes['mode'] != None:
			addElement(dom, key, 'mode', attributes['mode'])
	if attributes['beats'] != None:
		tNode = addElement(dom, attNode, 'time',
						   attr={'symbol': attributes['symbol']})
		addElement(dom, tNode, 'beats', text=attributes['beats'])
		addElement(dom, tNode, 'beat-type',
				   text=attributes['beat_type'])
	if attributes['instruments'] != None:
		addElement(dom, attNode, 'instruments',
				   text=attributes['instruments'])
	if attributes['staves'] != None:
		addElement(dom, attNode, 'staves',
				   text=attributes['staves'])
	if attributes['sign'] != None:
		clef = addElement(dom, attNode, 'clef')
		addElement(dom, clef, 'sign', text=attributes['sign'])
		if attributes['line'] != None:
			addElement(dom, clef, 'line', text=attributes['line'])
		if attributes['clef_octave_change'] != None:
			addElement(dom, clef, 'clef_octave_change',
					   text=attributes['clef_octave_change'])
	if attributes['directive'] != None:
		addElement(dom, attNode, 'directive',
				   text=attributes['directive'])
	if attributes['chromatic'] != None:
		transpose = addElement(dom, attNode, 'transpose')
		addElement(dom, transpose, 'chromatic',
				   text=attributes['chromatic'])
		if attributes['diatonic']:
			addElement(dom, transpose, 'diatonic',
					   text=attributes['diatonic']) 
		if attributes['octave_change']:
			addElement(dom, transpose, 'octave-change',
					   text=attributes['octave_change']) 
		if attributes['double_']:
			addElement(dom, transpose, 'double',
					   text=attributes['double_']) 


def addDirection(dom, mNode, direction):
	dirNode = addElement(dom, mNode, 'direction')
	typeNode = addElement(dom, dirNode, 'direction-type')
	addElement(dom, typeNode, direction['type'],
			   text=direction['value'])


def addBarline(dom, mNode, barline):
	barNode = addElement(dom, mNode, 'barline',
						 attr={'location': barline['location']})
	addElement(dom, barNode, 'bar-style', text=barline['style'])


def getTableItem(table, index, cursor):
	command = 'SELECT * FROM %s WHERE row_id = %s' % (table, index)
	cursor.execute(command)
	result = cursor.fetchone()
	desc = dtuple.TupleDescriptor([[f][0] for f
								   in cursor.description])
	return dtuple.DatabaseTuple(desc, result)


def nodeXML(node, indent):
	output = StringIO.StringIO()
	node.writexml(output, indent=indent, addindent='\t', newl='\n')
	stringout = output.getvalue().strip()
	stringout = re.sub(r'>\n\t+([^\n\t<]+)\n\t+<', r'>\1<', stringout)
	return indent + stringout + '\n'
	

def fetchrows(cursor):
	rows = cursor.fetchall()
	if not rows: return
	new_rows = [[] for x in rows]
	desc = dtuple.TupleDescriptor([[f][0] for f
								   in cursor.description])
	for i in range(len(rows)):
		new_rows[i] = dtuple.DatabaseTuple(desc, rows[i])
	return new_rows
	

def addElement(doc, node, elementName, text='', attr={}):
	child = doc.createElement(elementName)
	node.appendChild(child)
	for key, value in attr.items():
		child.attributes[key] = str(value)
	text = str(text)
	if text != '':
		text = re.sub('&amp;', '&', text)
		text = repr(text)
		text = re.sub("(^u?'|'$)", '', text)   
		textChild = doc.createTextNode(text)
		child.appendChild(textChild)
	return child


def usageError(errorText=''):
	if errorText:
		warn(errorText + '\n')
	warn("Usage: export_xml --database dbfile " + \
					 "--path path file [file2...]\n\n")
	sys.exit(2)


def export(printer=sys.stdout, **options):
	global StartTime
	StartTime = time.time()
	db = sqlbackend.connect(**options)
	cursor = db.cursor()

	if 'path' not in options:
		sys.exit("musicsql.database.exportxml.export: the 'path' argument is required.")
	command = "SELECT * FROM files WHERE path = '%s';"
	cursor.execute(command % options['path'])
	fileInfo = fetchrows(cursor)
	if fileInfo:
		fileInfo = fileInfo[0]
	else:
		sys.exit('Error: that file is not in the database!\n')
	if 'parts' in options:
		attrs = ', '.join(["'%s'" % x for x in options['parts'].split(',')])
		command = ('SELECT * FROM parts WHERE file_id = %%s ' +
				   'AND part_attr IN (%s)') % attrs
	else:
		command = 'SELECT * FROM parts WHERE file_id = %s'
	cursor.execute(command % fileInfo['row_id'])
	parts = fetchrows(cursor)
	if 'ticks' in options:
		bounds = [int(x) for x in options['ticks'].split('-')]
		command = '''
		  SELECT _start_tick, number
		  FROM measures
		  WHERE file_id = %s
		  ORDER BY _start_tick
		''' % fileInfo['row_id']
		cursor.execute(command)
		results = cursor.fetchall()
		arr = [x[1] for x in results if int(x[0]) <= bounds[0]]
		bounds[0] = int(arr[-1])
		arr = [x[1] for x in results if int(x[0]) <= bounds[1]]
		bounds[1] = int(arr[-1])
		measures = '%d-%d' % tuple(bounds)
	else:
		measures = options.get('measures', None)
	output = '''<?xml version="1.0" standalone="no"?>
<!DOCTYPE score-partwise PUBLIC "-//Recordare//DTD MusicXML 1.0 Partwise//EN" "http://www.musicxml.org/dtds/partwise.dtd">
<score-partwise>
'''
	printer.write(output)
	dom = xml.dom.minidom.Document()
	printer.write(fileHeader(dom, fileInfo, parts, cursor))
	highlighted = options.get('highlighted', None)
	addParts(dom, fileInfo, parts, db, cursor, options['backend'],
			 measures, highlighted, printer)
	printer.write('</score-partwise>\n')
	warn('Completed in %d seconds.\n' % (time.time() - StartTime))


def exportableToXml(row, tmp_dir, **options):
	from os import fdopen
	import tempfile

	try:
		options['path'] = row['_file']
		options['measures'] = row['_measures']
		options['parts'] = row['_parts']
		options['highlighted'] = row['_noteheads']
	except KeyError:
		sys.exit("Error: Unable to locate the extra MusicSQL output " +
			 "headers.\nAre you sure you exported this from " +
			 "MusicSQL using the 'exportable' option?\n")
	fileinfo = tempfile.mkstemp(dir=tmp_dir)
	tmp_handle = fdopen(fileinfo[0], 'w')
	tmp_file = fileinfo[1]
	export(printer=tmp_handle, **options)
	tmp_handle.close()
	return tmp_file


if __name__ == '__main__':
	import musicsql.options
	opts = ['backend=', 'database=', 'password?', 'file=', 'measures=?', 'parts=?',
			'highlighted=?']
	options, args = musicsql.options.getOptions(opts)
	try:
		export(**options)
	except KeyboardInterrupt:
		sys.exit(2)

#####
old_code = '''
		if note['slur_stop']:
			addElement(dom, notations, 'slur',
					   attr={'type': 'stop',
							 'number': note['slur_stop']})
		if note['slur_start']:
			addElement(dom, notations, 'slur',
					   attr={'type': 'start',
							 'number': note['slur_start']})
'''
