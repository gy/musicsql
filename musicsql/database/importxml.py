#!/usr/bin/python

# import_file.py
#
# Imports MusicXML files into a relational database.
#
# Initially coded in Perl by Bret Aarden, January 2005.
# Re-coded and expanded in Python, June 2005.
# Adapted into the musicsql package, March 2007

# To Do:
# - Assemble all measure objects in memory, then dump to database all
#   at once (avoids costly UPDATEs)

# Known BUGS:
#
# - The algorithm for determining the previous note is (necessarily?)
#   flawed. The nearest previous pitch is not necessarily the correct
#   one.

# MusicXML notes:
# The following elements are currently ignored silently:
#   tied
#   tuplet
#   fermata
#   normal-type
#   normal-dot
#   lyric
#   print

import string
import re
import os
import sys
import glob
import getopt
from time import time
import traceback
import musicsql.backend as sqlbackend

import xml.dom.pulldom as pulldom
import xml.sax
from xml.dom import Node
from xml.sax.handler import ContentHandler, EntityResolver
import musicsql.dtuple as dtuple

warn = sys.stderr.write


### PullDOM methods

def processDocument(events, globals):
	cursor = globals['cursor']
	for (event, node) in events:
		if event != pulldom.START_ELEMENT:
			continue
		tag = node.nodeName
		if tag == "score-part":
			events.expandNode(node)
			fileDict, partDict = processScorePart(node, globals)
			if fileDict:
				updateItem('files', globals['fileID'], fileDict, globals)
			id = insertItem('parts', partDict, globals)
			attr = partDict['part_attr']
			globals['partID'][attr] = id
		elif tag == "part":
			if globals.has_key('measureEnd'):
				del globals['measureEnd']
				del globals['beatLen']
			globals['part'] = node.getAttribute('id')
			globals['ticks'] = 0
			globals['attrID'] = None
			globals['prevNote'] = {}
			globals['prevSemit'] = {}
			globals['prevBeat'] = {}
			globals['wedges'] = {}
			attributes = {}
		elif tag == "measure":
			events.expandNode(node)
			processMeasure(node, attributes, globals)
		elif tag == "work":
			events.expandNode(node)
			number = title = ''
			child = node.getElementsByTagName('work-number')
			if child:
				number = child[0].firstChild.nodeValue
			child = node.getElementsByTagName('work-title')
			if child:
				title = child[0].firstChild.nodeValue
			dict = {'work_title': title, 'work_number': number}
			updateItem('files', globals['fileID'], dict, globals)
		elif tag == "movement-number":
			events.expandNode(node)
			dict = {'movement_number': node.firstChild.nodeValue}
			updateItem('files', globals['fileID'], dict, globals)
		elif tag == "movement-title":
			events.expandNode(node)
			if node.firstChild:
				dict = {'movement_title': node.firstChild.nodeValue}
				updateItem('files', globals['fileID'], dict, globals)
		elif tag == "score-timewise":
			warn("MusicXML files of type 'score-timewise'" \
							 + "cannot be processed.\n")
			sys.exit(2)
		elif isinstance(node.parentNode, Node):
			if node.parentNode.nodeName == "score-partwise":
				warn("score-partwise: '%s' " % tag \
								 + "node not processed.\n")


def processScorePart(node, globals):
	fileDict = {}
	partDict = {'file_id': globals['fileID'],
				'part_attr': node.getAttribute('id')}
	context = 'score-part(%s):' % partDict['part_attr']
	globals['context'] = context
	for child in node.childNodes:
		if child.nodeType != child.ELEMENT_NODE:
			continue
		tag = child.nodeName
		if tag == 'part-name':
			partDict['part_name'] = child.firstChild.nodeValue
		elif tag == 'group':
			partDict['part_group'] = child.firstChild.nodeValue
		elif tag == 'identification':
			processIdentification(child, fileDict, partDict,
								  context+'identification:', globals)
	return fileDict, partDict


def processIdentification(node, fileDict, partDict, context, globals):
	globals['context'] = context
	for child in node.childNodes:
		if child.nodeType != child.ELEMENT_NODE:
			continue
		tag = child.nodeName
		if tag == 'rights':
			fileDict['rights'] = child.firstChild.nodeValue
		elif tag == 'source':
			fileDict['source'] = child.firstChild.nodeValue
		elif tag == 'encoding':
			fieldList = ['encoding-date', 'encoder']
			getNodeElements(child, fileDict, context+'encoding',
							globals, textTags=fieldList)
	return fileDict


def processMeasure(node, attributes, globals):
	'''
	Every measure gets its own entry in the measures table.
	(A lot of information gets repeated to aid in searching.)
	First this info is updated by the 'attributes' element, if any.
	'''
	if (globals.has_key('measureEnd') and 
		globals['ticks'] != globals['measureEnd']):
		context = "part(%s):measure(%s)" % (globals['part'],
											 globals['measureNumber'])
		warn('Warning: measure of irregular length: %s.\n' % context)
#		globals['ticks'] = globals['measureEnd']
	measureDict = {'number': node.getAttribute('number'),
				   'file_id': globals['fileID']}
	measureProperties = measureDict.copy()
	measureProperties['_start_tick'] = globals['ticks']
	globals['measureStart'] = globals['ticks']
	updateGlobals(attributes, measureProperties, globals)	
	globals['measureNumber'] = measureProperties['number']
	context = "part(%s):measure(%s):" % (globals['part'],
										 globals['measureNumber'])
	globals['context'] = context
	updateImportProgress(globals)
	globals['measureID'] = insertUniqueItem('measures',
											measureDict,
											measureProperties,
											globals)
	for child in node.childNodes:
		if child.nodeType != child.ELEMENT_NODE:
			continue
		tag = child.nodeName
		if tag == 'attributes':
			processAttributes(child, attributes, measureProperties, globals,
							  context+'attributes:')
			updateGlobals(attributes, measureProperties, globals)
		elif tag == 'direction':
			processDirection(child, globals, context+'direction:')
		elif tag == 'note':
			processNote(child, attributes, globals,
						context+'note:')
		elif tag == 'forward':
			processNote(child, attributes, globals,
						context+'note:', rest=True)
		elif tag == 'backup':
			duration = child.getElementsByTagName('duration')[0]
			globals['ticks'] -= int(duration.firstChild.nodeValue) \
								* globals['divisionRatio']
		elif tag == 'barline':
			styles = child.getElementsByTagName('bar-style')
			barDict = {'location': child.getAttribute('location'),
					   'event_id': insertEvent(globals)}
			if styles:
				barDict['style'] = styles.item(0).firstChild.nodeValue
			testDict = {'event_id': barDict['event_id']}
			insertUniqueItem('barlines', testDict, barDict, globals)
		elif tag == 'print':
			pass
		else:
			warn("%s '%s' node not processed.\n" % (context, tag))

def insertEvent(globals, ticks=None):
	def newBeat(globals):
		if globals['ticks'] > globals['measureEnd']:
			warn('Error: notehead extends into next measure! (%s)'
				 % globals['context'])
			sys.exit(1)
#		if globals['ticks'] == globals['measureEnd']:
#			beat = 1.0
		else:
			offset = float(globals['ticks'] - globals['measureStart'])
			beat = 1.0 + offset / float(globals['beatLen'])
			return beat

	moment_id = insertMoment(globals, ticks)
	part_id = globals['partID'][globals['part']]
	eventDict = {'moment_id': moment_id, 'part_id': part_id}
	if globals['ticks'] == globals['measureStart']:
		event = getTableItem('events', eventDict, globals)
		if event:
			if globals.has_key('beatLen'):
				updateDict = {'beat': newBeat(globals),
							  'measure_id': globals['measureID']}
				updateItem('events', event['row_id'], updateDict, globals)
			return event['row_id']
	eventInfo = eventDict.copy()
	eventInfo['file_id'] = globals['fileID']
 #   if globals['ticks'] < globals.get('measureEnd'):
	eventInfo['measure_id'] = globals['measureID']
	if globals.has_key('beatLen'):
		eventInfo['beat'] = newBeat(globals)
	if globals['attrID']:
		eventInfo['attribute_id'] = globals['attrID']
	id = insertUniqueItem('events', eventDict, eventInfo, globals)
	return id

def insertMoment(globals, ticks=None):
	if not ticks:
		ticks = globals['ticks']
	momentDict = {'file_id': globals['fileID'], 'ticks': ticks}
	id = insertUniqueItem('moments', momentDict, momentDict, globals)
	return id

def updateGlobals(attributes, measureProperties, globals):
	if (globals['ticks'] != globals['measureStart'] 
		or not attributes.has_key('beats')):
		return
	quartersPerMeasure = (float(attributes['beats']) * 4
						  / float(attributes['beat-type']))
	globals['beatLen'] = (globals['divisions'] * quartersPerMeasure
						  / float(attributes['beats']))
	if int(attributes['beats']) % 3 == 0 and int(attributes['beats']) > 3:
		globals['beatLen'] *= 3
	endTick = int(globals['ticks'] + 
				  globals['divisions'] * quartersPerMeasure)
	globals['measureEnd'] = endTick


def updateImportProgress(globals):
	globals['currentMeasure'] += 1
	progress = 4 + int(92 * globals['currentMeasure']
					   / globals['measureCount'])
	if progress != globals['progress']:
		warn("%3d%%\n" % (progress + 0))
		globals['progress'] = progress


def processAttributes(node, attributes, measureProperties, globals, context):
	globals['context'] = context
	attributes['_start_tick'] = globals['ticks']
	attributes['file_id'] = globals['fileID']
	part = globals['part']
	event_id = insertEvent(globals)
	attrDict = {'startevent_id': event_id}
	attributes['startevent_id'] = event_id
	for child in node.childNodes:
		if child.nodeType != child.ELEMENT_NODE:
			continue
		tag = child.nodeName
		if tag == 'directive':
			value = child.firstChild.nodeValue
			attributes['directive'] = value
		elif tag == 'divisions':
			globals['divisionRatio'] = globals['divisions'] / \
									   int(child.firstChild.nodeValue)
		elif tag == 'key':
			tags = ['fifths', 'mode']
			getNodeElements(child, attributes, context+'key:',
							globals, textTags=tags)
		elif tag == 'time':
			getNodeAttributes(child, ['symbol'], attributes)
			tags = ['beats', 'beat-type']
			getNodeElements(child, attributes, context+'time:',
							globals, textTags=tags)
			updateGlobals(attributes, measureProperties, globals)
		elif tag == 'instruments':
			value = child.firstChild.nodeValue
			attributes['instruments'] = value
		elif tag == 'staves':
			attributes['staves'] = child.firstChild.nodeValue
		elif tag == 'clef':
			tags = ['sign', 'line', 'clef-octave-change']
			getNodeElements(child, attributes, context+'clef:',
							globals, textTags=tags)
		elif tag == 'transpose':
			tags = ['diatonic', 'chromatic']
			getNodeElements(child, attributes,
							context+'transpose:', globals,
							textTags=tags, booleanTags=['double'])
			if attributes['double']:
				attributes['double_'] = attributes['double']
				del attributes['double']
		else:
			warn("%s '%s' was not processed.\n" % (context, tag))
	attribute_id = insertUniqueItem('attributes', attrDict,
									attributes, globals)
	globals['attrID'] = attribute_id
	updateItem('events', event_id, {'attribute_id': attribute_id},
			   globals)


def processNote(node, attributes, globals, context, rest=False):
	globals['context'] = context
	noteProperties = {}
	part = globals['part']
	noteheadProperties = {'voice': 1,
						  'start_tick': globals['ticks']}
	beamProperties = {}
	chord = False

	for child in node.childNodes:
		if child.nodeType != child.ELEMENT_NODE:
			continue
		tag = child.nodeName
		if tag == 'pitch':
			tags = ['step', 'alter', 'octave']
			getNodeElements(child, noteheadProperties,
							context+'pitch:', globals, tags)
			if noteheadProperties.get('alter'):
				noteheadProperties['alter_'] = noteheadProperties['alter']
				del noteheadProperties['alter']
		elif tag == 'rest':
			rest = True
		elif tag == 'duration':
			value = int(child.firstChild.nodeValue) \
					* globals['divisionRatio']
			noteheadProperties['notehead_duration'] = value
			noteProperties['duration'] = value
		elif tag == 'time-modification':
			tags = ['actual-notes', 'normal-notes',
					'normal-type', 'normal-dot']
			getNodeElements(child, noteheadProperties,
							context+'time-modification:', globals,
							tags)
			if noteheadProperties.has_key('normal-type'):
				del noteheadProperties['normal-type']
			if noteheadProperties.has_key('normal-dot'):
				del noteheadProperties['normal-dot']
		elif tag == 'tie':
			if child.getAttribute('type') == 'start':
				noteheadProperties['tie_start'] = 1
			else:
				noteheadProperties['tie_stop'] = 1
		elif tag == 'chord':
			chord = True
		elif tag == 'voice':
			noteheadProperties['voice'] = child.firstChild.nodeValue
		elif tag == 'type':
			noteheadProperties['type'] = child.firstChild.nodeValue
		elif tag == 'dot':
			if noteheadProperties.has_key('dot'):
				noteheadProperties['dot'] += 1
			else:
				noteheadProperties['dot'] = 1
		elif tag == 'accidental':
			value = child.firstChild.nodeValue
			noteheadProperties['accidental'] = value
		elif tag == 'stem':
			noteheadProperties['stem'] = child.firstChild.nodeValue
		elif tag == 'beam':
			index = 'beam%s' % child.getAttribute('number')
			beamProperties[index] = child.firstChild.nodeValue
		elif tag == 'grace':
			grace = True
			if globals.has_key('lastGraceTick') and \
				   globals['lastGraceTick'] == globals['ticks']:
				globals['graceCount'] += 1
			else:
				globals['graceCount'] = 1
				globals['lastGraceTick'] = globals['ticks']
			noteheadProperties['grace_order'] = globals['graceCount']
			noteheadProperties['notehead_duration'] = 0
			following = child.getAttribute('steal-time-following')
			previous = child.getAttribute('steal-time-previous')
			if following:
				noteheadProperties['steal_time'] = following
			if previous:
				noteheadProperties['steal_time'] = '-' + previous
		elif tag == 'notations':
			processNoteNotations(child, noteheadProperties,
								 context+'notations:', globals)
		elif tag == 'lyric':
			pass
		else:
			warn("%s '%s' not processed.\n"
							 % (context, tag))

	# Do all of the order- and context-sensitive processing
	if rest:
		noteheadProperties['rest'] = True
	else:
		transpose(noteProperties, noteheadProperties, attributes)
		try:
			noteProperties['semit'] = noteToSemit(noteheadProperties)
		except ValueError, str:
			warn('Error: %s: %s.\n' % (str, globals['context']))
			sys.exit(2)
		noteProperties['pc'] = noteProperties['semit'] % 12
	if chord:
		globals['ticks'] -= noteheadProperties['notehead_duration']
		noteheadProperties['start_tick'] = globals['ticks']
	noteheadID = addNotehead(noteProperties, noteheadProperties,
							 globals)
#	addSlur(noteheadProperties, noteheadID, globals)
	if len(beamProperties) > 0:
		beamProperties['notehead_id'] = noteheadID
		insertItem('beams', beamProperties, globals)


def transpose(note, notehead, attributes):
	stepdigit = {'C': 0, 'D': 1, 'E': 2, 'F': 3,
				 'G': 4, 'A': 5, 'B': 6}
	if not attributes.has_key('chromatic'):
		note['concert_step'] = stepdigit[notehead['step']]
		note['concert_octave'] = notehead['octave']
		if notehead.has_key('alter_'):
			note['concert_alter'] = notehead['alter_']
		return
	#	 diatonic: {chromatic: base40delta}
	intervals = {0: {0: 0, 1: 1},
				 1: {0: 4, 1: 5, 2: 6, 3: 7},
				 2: {2: 10, 3: 11, 4: 12, 5: 13},
				 3: {4: 16, 5: 17, 6: 18},
				 4: {6: 22, 7: 23, 8: 24},
				 5: {7: 27, 8: 28, 9: 29, 10:30},
				 6: {9: 33, 10: 34, 11: 35, 12: 36},
				 7: {11: 39, 12: 40}}
	defaultDiatonic = {0: 0, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3,
					   6: 3, 7: 4, 8: 5, 9: 5, 10: 6, 11: 6}
	diatones = [3, 9, 15, 20, 26, 32, 38]
	stepToBase40 = {'C': 3, 'D': 9, 'E': 15, 'F': 20,
					'G': 26, 'A': 32, 'B': 38}
	base40ToStep = {3: 'C', 9: 'D', 15: 'E', 20: 'F',
					26: 'G', 32: 'A', 38: 'B'}
	chromatic = int(attributes['chromatic'])
	if attributes.has_key('diatonic'):
		diatonic = int(attributes['diatonic'])
	else:
		diatonic = defaultDiatonic[abs(chromatic)]
	sign = cmp(chromatic, 0)
	base40delta = sign * intervals[abs(diatonic)][abs(chromatic)]
	base40 = stepToBase40[notehead['step']] + \
			 int(notehead['alter_'] or 0)
	base40transpose = (base40 + base40delta) % 40
	base40diatone = [x for x in diatones
					 if x <= base40transpose + 2][-1]
	note['concert_step'] = stepdigit[base40ToStep[base40diatone]]
	note['concert_alter'] = base40transpose - base40diatone
	note['concert_octave'] = notehead['octave']
	if sign > 0 and base40transpose < base40:
		note['concert_octave'] += 1
	elif sign < 0 and base40transpose > base40:
		note['concert_octave'] -= 1
	if attributes.has_key('octave-change'):
		note['concert_octave'] += attributes['octave-change']
		
def addNotehead(noteProperties, noteheadProperties, globals):
	grace = False
	noteID = None
	attrID = globals['attrID']
	eventID = insertEvent(globals)
	noteheadProperties['startevent_id'] = eventID
	if noteheadProperties.has_key('grace_order'):
		grace = True
	else:
		globals['ticks'] += noteheadProperties['notehead_duration']
		endEventID = insertEvent(globals)
		noteheadProperties['endevent_id'] = endEventID
	index = ''
	if noteProperties.has_key('semit'):
		index = '%s:%s' % (noteheadProperties['voice'],
						   noteProperties['semit'])
	start_notehead = not noteheadProperties.has_key('tie_stop')
	if not start_notehead:
		if not index:
			warn('Warning: tie attached to pitchless note (%s).\n' \
				 % globals['context'])
		else:
			value = findIndex(globals['tiePrevNotehead'], index)
			noteheadProperties['tiedto_id'] = value
	if not grace and not noteheadProperties.has_key('rest'):
		noteID = addNote(noteProperties, noteheadProperties, globals)
		noteheadProperties['note_id'] = noteID
	del noteheadProperties['start_tick']
	noteheadID = insertItem('noteheads', noteheadProperties, globals)
	if noteheadProperties.has_key('tie_start'):
		if not index:
			warn('Warning: tie attached to pitchless note (%s).\n' \
				 % globals['context'])
		else:
			globals['tiePrevNotehead'][index] = noteheadID
	if noteID and start_notehead:
		updateProperties = {'startnotehead_id': noteheadID}
		updateItem('notes', noteID, updateProperties, globals)
	return noteheadID


def findIndex(map, index):
	found = True
	if not map.has_key(index):
		(voice, semit) = string.split(index, ':')
		for key in map.iterkeys():
			(v, s) = string.split(index, ':')
			if s == semit:
				index = key
				break
	value = map[index]
	del map[index]
	return value
	

def addNote(noteProperties, noteheadProperties, globals):
	voice = noteheadProperties['voice']
	isBeat = False
	if noteProperties.has_key('semit'):
		index = '%s:%s' % (voice, noteProperties['semit'])
	noteProperties['startevent_id'] = noteheadProperties['startevent_id']
	noteProperties['endevent_id'] = noteheadProperties['endevent_id']
	if noteheadProperties.has_key('tie_stop'):
		noteID = findIndex(globals['tieStarts'], index)
		tiedNoteProperties = getTableItem('notes', {'row_id': noteID},
										  globals)
		duration = tiedNoteProperties['duration'] + \
				   noteProperties['duration']
		updateProperties = {'endevent_id': noteProperties['endevent_id'],
							'duration': duration}
		updateItem('notes', noteID, updateProperties, globals)
	else:
		measureLen = globals['measureEnd'] - globals['measureStart']
		semit = noteProperties['semit']
		best = {}
		if globals['prevNote'].has_key(voice):
			list = globals['prevNote'][voice]
			best = findBestPrevVoice(list, semit, globals)
			if best.has_key('noteID'):
				noteProperties['prevnote_id'] = best['noteID']
				list.pop(best['index'])
				if best['semitDiff'] != 0:
					globals['prevSemit'][voice].pop(0)
			noteProperties['prevsemit_id'] = globals['prevSemit'][voice][0]
		measureOffset = noteheadProperties['start_tick'] - globals['measureStart']
		isBeat = measureOffset % globals['beatLen'] == 0
		if isBeat and globals['prevBeat'].has_key(voice):
			semit = noteProperties['semit']
			prevBeat = noteheadProperties['start_tick'] - globals['beatLen']
			list = globals['prevBeat'][voice]
			l = len(list)
			for n in range(l-1, -1, -1):
				if noteheadProperties['start_tick'] - list[n][0] > measureLen + 1:
					del list[n]
			l2 = [(abs(semit - list[n][2]), list[n][3], n)
				  for n in range(0, len(list))
				  if list[n][0] <= prevBeat and list[n][1] > prevBeat]
			if l2:
				l2.sort()
				noteProperties['prevbeat_id'] = l2[0][1]
				del list[l2[0][2]]
		noteID = insertItem('notes', noteProperties, globals)
		if globals['prevNote'].has_key(voice) and best.has_key('semitDiff'):
			if best['semitDiff'] != 0:
				globals['prevSemit'][voice].append(noteID)				
	if not globals['prevNote'].has_key(voice):
		globals['prevNote'][voice] = []
	if not globals['prevSemit'].has_key(voice):
		globals['prevSemit'][voice] = ['', noteID]
	endTick = noteheadProperties['start_tick'] + noteProperties['duration']
	indexList = (endTick, noteProperties['semit'], noteID)
	globals['prevNote'][voice].append(indexList)
	if isBeat:
		indexList = (noteheadProperties['start_tick'], endTick,
					 noteProperties['semit'], noteID)
		if not globals['prevBeat'].has_key(voice):
			globals['prevBeat'][voice] = []
		globals['prevBeat'][voice].append(indexList)
	if noteheadProperties.has_key('tie_start'):
		globals['tieStarts'][index] = noteID
	return noteID


def findBestPrevVoice(list, semit, globals):
	measureLen = globals['measureEnd'] - globals['measureStart']
	(TICK, SEMIT, NOTEID, INDEX) = (0, 1, 2, 3)
	l = len(list)
	best = {}
	for n in range(l-1, -1, -1):
		if globals['ticks'] - list[n][TICK] > measureLen + 1:
			del list[n]
	l2 = [(-list[n][TICK], abs(semit - list[n][SEMIT]),
		   list[n][NOTEID], n)
		  for n in range(0, len(list))
		  if list[n][TICK] < globals['ticks']]
	if l2:
		l2.sort()
		best = {'semitDiff': l2[0][SEMIT], 'noteID': l2[0][NOTEID],
				'index': l2[0][INDEX]}
	return best


def addSlur(noteheadProperties, noteheadID, globals):
	if globals.has_key('slur_stop'):
		while globals['slur_stop']:
			number = globals['slur_stop'].pop()
			startNote = globals['slurs'][number]
			dict = {'startnotehead_id': startNote,
					'stopnotehead_id': noteheadID}
			insertItem('slurs', dict, globals)
			del globals['slurs'][number]
		del globals['slur_stop']
	if globals.has_key('slur_start'):
		while globals['slur_start']:
			number = globals['slur_start'].pop()
			globals['slurs'][number] = noteheadID
		del globals['slur_start']
		

def processNoteNotations(node, noteheadProperties, context, globals):
	globals['context'] = context
	for child in node.childNodes:
		if child.nodeType != child.ELEMENT_NODE:
			continue
		tag = child.nodeName
		if tag == 'dynamics':
			dict = {}
			getNodeElements(child, dict, context+'dynamics:', globals,
							textTags=['other-dynamics'],
							booleanTags=['*'])
			for key, value in dict.items():
				if key == 'other-dynamics':
					noteheadProperties['dynamics'] = 'other:' + value
				else:
					noteheadProperties['dynamics'] = key
		elif tag == 'slur':
			# The main processing gets handled in processNote()
			type = child.getAttribute('type')
			number = child.getAttribute('number')
			index = 'slur_' + type
			## INSERT THIS INTO A NOTATIONS TABLE
			return
			if not globals.has_key(index):
				globals[index] = []
			globals[index].append(number)
			if noteheadProperties.has_key('index'):
				noteheadProperties[index] += ',' + number
			else:
				noteheadProperties[index] = number
		elif tag == 'articulations':
			dict = {}
			getNodeElements(child, dict, context+'articulations:',
							globals, textTags=['other-articulation'],
							booleanTags=['*'])
			for key, value in dict.items():
				if key == 'other-articulation':
					noteheadProperties['articulation'] = 'other:' \
														 + value
				else:
					noteheadProperties['articulation'] = key
		elif tag == 'fermata':
			pass
		elif tag == 'tied':
			pass
		elif tag == 'tuplet':
			pass
		else:
			warn("%s '%s' node not processed.\n"
							 % (context, tag))


def processDirection(node, globals, context):
	globals['context'] = context
	for child in node.childNodes:
		if child.nodeType != child.ELEMENT_NODE:
			continue
		tag = child.nodeName
		if tag == 'direction-type':
			processDirection(child, globals,
							 context+'direction-type:')
		elif tag == 'dynamics':
			dynamics = {}
			getNodeElements(child, dynamics, context+'dynamics:',
							globals, textTags=['other-dynamics'],
							booleanTags=['*'])
			dict = {'event_id': insertEvent(globals)}
			for key, value in dynamics.items():
				if key == 'other-dynamics':
					dict['type'] = key
					dict['value'] = value
				else:
					dict['type'] = 'dynamics'
					dict['value'] = key
			insertItem('directions', dict, globals)
		elif tag == 'wedge':
			type = child.getAttribute('type')
			number = child.getAttribute('number') or 1
			event = insertEvent(globals)
			if type == 'stop':
				if globals['wedges'].has_key(number):
					wedge = globals['wedges'][number]
					wedge['stopevent_id'] = event
					insertItem('wedges', wedge, globals)
					del globals['wedges'][number]
				else:
					warn("XML error: inconsistent use of 'wedge' " +
						 "elements. (%s)\n" % context)
			else:
				partID = globals['partID'][globals['part']]
				wedge = {'type': type, 'startevent_id': event}
				globals['wedges'][number] = wedge
		elif tag == 'words' or tag == 'rehearsal':
			dict = {'part_id': globals['partID'][globals['part']],
					'event_id': insertEvent(globals),
					'start_tick': globals['ticks'], 'type': tag,
					'value': child.firstChild.nodeValue}
			insertItem('directions', dict, globals)
		else:
			warn("%s '%s' node not processed.\n"
							 % (context, tag))


def getNodeElements(node, dict, context, globals,
					textTags=None,	 # get its CDATA
					booleanTags=None): # note its presence
	globals['context'] = context
	for child in node.childNodes:
		if child.nodeType != child.ELEMENT_NODE:
			continue
		tag = child.nodeName
		if textTags and (tag in textTags):
			dict[tag] = ''
			for textnode in child.childNodes:
				dict[tag] += textnode.nodeValue
		elif booleanTags and (tag in booleanTags
							  or '*' in booleanTags):
			# booleanTags will match any tag if it equals ['*'].
			dict[tag] = 1
		else:
			warn("%s '%s' element was not processed.\n"
							 % (context, tag))


def getNodeAttributes(node, tags, dict):
	for tag in tags:
		if dict.has_key(tag):
			del dict[tag]
		value = node.getAttribute(tag)
		if value:
			dict[tag] = value


### SAX Handler classes

class LCMObject:
	def __init__(self):
		self.result = 1

	def addNumber(self, n):
		if (not n): n = 1
		m = self.result
		n = int(n)
		m = int(m)
		if (n > m): a, b = n, m
		else: a, b = m , n
		while b:
			a, b = b, a % b
		if (a == 0): a = 1
		self.result = n * m / a


class LocalEntityResolver(EntityResolver):
	def __init__(self, localPath=''):
		self.localPath = localPath
		self.pattern = re.compile('.*\W(?=\w+\.dtd)')
	def resolveEntity(self, publicID, systemID):
		from os.path import join
		if self.localPath:
			systemID = join(self.localPath,
							self.pattern.sub('', systemID))
		return EntityResolver.resolveEntity(self, publicID, systemID)


class DivisionsHandler(ContentHandler):
	def __init__(self, lcmObject, lineCount):
		self.divisions = lcmObject
		self.lineCount = lineCount
		self.inDivisionsTag = False
		self.locator = None
		self.progress = -1
	def setDocumentLocator(self, locator):
		self.locator = locator
	def startElement(self, name, attrs):
		if name == 'divisions':
			self.inDivisionsTag = True
			self.chars = ''
	def characters(self, ch):
		if self.inDivisionsTag:
			self.chars += ch
	def endElement(self, name):
		self.inDivisionsTag = False
		if name == 'divisions':
			self.divisions.addNumber(self.chars)
			progress = int(5 * self.locator.getLineNumber() /
						   self.lineCount)
			if progress != self.progress:
				warn("%3d" % (progress+0) + "%\n")
				self.progress = progress


### SQL functions


def getTableItem(table, idDict, globals):
	l = sqlbackend.assignment_list(idDict.keys(), globals['backend'])
	query = ' AND '.join(l)
	command = "SELECT * FROM %s WHERE %s" % (table, query)
	cursor = globals['cursor']
	cursor.execute(command, idDict)
	result = cursor.fetchone()
	if not result:
		return None
	desc = dtuple.TupleDescriptor(map(lambda x: (string.lower(x[0]),) + x[1:],
									  cursor.description))
	dt = dtuple.DatabaseTuple(desc, result)
	return dt


def fetchrows(cursor):
	rows = cursor.fetchall()
	if not rows: return
	desc = dtuple.TupleDescriptor([[f][0] for f
								   in cursor.description])
	for i in range(len(rows)):
		rows[i] = dtuple.DatabaseTuple(desc, rows[i])
	return rows


def updateItem(table, index, dict, globals):
	'''
	Use the items in a dictionary to update an SQL table entry.
	'''
	fullDict = {}
	for k, v in dict.iteritems():
		fullDict[k.replace('-', '_')] = v	  
	row_id = sqlbackend.assignment_list(['row_id'], globals['backend'])
	l = sqlbackend.assignment_list(fullDict.keys(), globals['backend'])
	query = ', '.join(l)
	command = 'UPDATE %s SET %s WHERE %s' % (table, query, row_id[0])
	fullDict['row_id'] = index
	cursor = globals['cursor']
	cursor.execute(command, fullDict)


def insertItem(table, dict, globals):
	'''
	Use the items in a dictionary to create a new SQL table entry.
	'''
	fullDict = {}
	for (k, v) in dict.iteritems():
		fullDict[k] = v
		if v == '':
			fullDict[k] = None
	fields = ', '.join([k.replace('-', '_')
						for k in fullDict.iterkeys()])
	p = sqlbackend.parameter_list(fullDict.keys(), globals['backend'])
	params = ', '.join(p)
	command = 'INSERT INTO %s (%s) VALUES (%s)' % (table, fields, params)
	cursor = globals['cursor']
	errors = globals['errors']
	try:
		cursor.execute(command, fullDict)
	except errors.get('IntegrityError'), err:
		warn("Error: item already exists in '%s' table.\n" % table)
		raise
		sys.exit(1)
	return sqlbackend.lastrowid(cursor, table, globals['backend'])


def insertUniqueItem(table, idDict, dataDict, globals):
	result = getTableItem(table, idDict, globals)
	if result == None:
		return insertItem(table, dataDict, globals)
	itemID = int(result['row_id'])
	updateItem(table, itemID, dataDict, globals)
	return itemID


### MAIN PROGRAM

def noteToSemit(dict):
	steps = {'C': 0, 'D': 2, 'E': 4, 'F': 5, 'G': 7, 'A': 9, 'B': 11}
	semit = (int(dict['octave']) - 4) * 12
	semit += int(steps[dict['step']])
	if dict.has_key('alter_'):
		semit += int(dict['alter_'])
	return semit


def addIntersects(db, globals):
	cursor = globals['cursor']
	fileid = sqlbackend.parameter_list(['fileID'], globals['backend'])
	command = '''
	  SELECT moments.ticks, moments.row_id
	  FROM moments
	  WHERE moments.file_id = %s
	  ORDER BY moments.ticks;
	''' % fileid[0]
	cursor.execute(command, globals)
	moments = cursor.fetchall()

	p = sqlbackend.parameter_list(['moment_id', 'note_id'],
								  globals['backend'])
	param = ', '.join(p)
	insert_command = 'INSERT INTO intersects (moment_id, note_id) ' \
					 + 'VALUES (%s);' % param
	oldProgress = 96
	command = '''
	  SELECT startmoment.ticks, stopmoment.ticks, notes.row_id
	  FROM notes
	  JOIN events as start ON notes.startevent_id = start.row_id
	  JOIN moments as startmoment ON startmoment.row_id = start.moment_id
	  JOIN events as stop ON notes.endevent_id = stop.row_id
	  JOIN moments as stopmoment ON stopmoment.row_id = stop.moment_id
	  WHERE start.file_id = %s
	  ORDER BY startmoment.ticks, stopmoment.ticks;
	''' % fileid[0]
	cursor.execute(command, globals)
	notes = cursor.fetchall()
	
	rows = []
	begin = newBegin = oldProgress = 0
	for i in range(len(moments)):
		(momentTick, momentID) = moments[i]
		progress = int(96 + 5 * i / len(moments))
		if progress > oldProgress:
			warn('%3d%%\n' % progress)
			oldProgress = progress
		if newBegin > begin:
			begin = newBegin
		newBegin = None
		for j in range(begin, len(notes)):
			(noteStart, noteEnd, noteID) = notes[j]
			if momentTick < noteStart:
				break
			if momentTick >= noteEnd:
				continue
			if newBegin == None:
				newBegin = j
			d = {'moment_id': int(momentID), 'note_id': int(noteID)}
			rows.append(d)
	# Because pyformat is broken in executemany() in MySQL:
	for row in rows:
		cursor.execute(insert_command, row)


def scanFile(filename, globals):
	lines = measures = 0
	try:
		textf = open(filename, 'r')
	except IOError:
		warn('Cannot open %s.\n' % filename)
		sys.exit(2)
	for line in textf.readlines():
		lines += 1
		if string.find(line, '<measure') >= 0:
			measures += 1
		if string.find(line, '&') >= 0:
			warn('Warning: encountered an ampersand ' + \
							 '(line %d).\n'  % lines)
		try:
			unicode(line, 'ascii')
		except UnicodeDecodeError:
			warn('Warning: non-ASCII character encountered ' +
				 '(line %d).\n' % lines)
		globals['measureCount'] = measures
	return lines


def findDivisions(filename, lines, globals, cursor):
	divisions = LCMObject()
	handler = DivisionsHandler(lcmObject=divisions, lineCount=lines)
	resolver = LocalEntityResolver(globals['dtdPath'])
	parser = xml.sax.make_parser()
	parser.setContentHandler(handler)
	parser.setEntityResolver(resolver)
	try:
		parser.parse(filename)
	except IOError, (errno, strerror):
		if errno == 'socket error' or errno == 'url error':
			warn("Error: Unable to access the DTD referenced in " +
				 "the XML file.\nConsider the --pathtodtd option " +
				 "to reference a local copy.\n")
		else:
			warn("I/O error(%s): %s\n" % (errno, strerror))
		raise
		sys.exit(2)
	except xml.sax._exceptions.SAXParseException, strerror:
		context = globals.get('context', '')
		warn("XML parsing error: %s (%s)\n" % (strerror, context))
		sys.exit(2)
	updateItem('files', globals['fileID'],
			   {'divisions': divisions.result}, globals)
	globals['divisions'] = divisions.result


def importFile(filename, globals):
	resolver = LocalEntityResolver(globals['dtdPath'])
	parser = xml.sax.make_parser()
	parser.setEntityResolver(resolver)
	events = pulldom.parse(filename, parser=parser)
	try:	
		processDocument(events, globals)
	except IOError, (errno, strerror):
		warn("I/O error(%s): %s\n" % (errno, strerror))
		sys.exit(2)


def processFile(filename, db, backend):
	start_time = time()
	cursor = db.cursor()
	errors = sqlbackend.get_errors(backend)
	import musicsql
	dtdPath = os.path.join(os.path.dirname(musicsql.__file__), 'dtd')
	globals = {'tieStarts': {}, 'tiePrevNotehead': {}, 'slurs': {},
			   'wedges': {}, 'partID': {}, 'cursor': cursor,
			   'progress': -1, 'currentMeasure': 0, 'errors': errors,
			   'dtdPath': dtdPath, 'backend': backend}
	try:
		lastrowid = insertItem('files', {'path': filename}, globals)
	except (errors.get('OperationalError'),
			errors.get('DatabaseError')):
		warn("SQL error: '%s' already exists in the database.\n" %
			 filename)
		sys.exit(2)
	globals['fileID'] = lastrowid

	warn("Scanning file...\n")
	lines = scanFile(filename, globals)
	
	warn("Finding a common division across parts...\n")
	findDivisions(filename, lines, globals, cursor)

	warn("Importing music...\n")
	globals['descriptors'] = {}
	importFile(filename, globals)
	db.commit()

	warn("Finding note/event intersects...\n")
	addIntersects(db, globals)
	db.commit()
	cursor.close()
	warn("Completed in %d seconds.\n" %
					 (time() - start_time))
	
def importxml(*files, **options):
	db = sqlbackend.connect(**options)
	fileList = []
	for file in files:
		fileList += glob.glob(file)
	for filename in fileList:
		warn("Processing '%s'...\n" % filename)
		processFile(filename, db, options['backend'])
	db.close()
