#!/usr/bin/python

import sys
from sqlalchemy import *
import musicsql.backend

def setup(**options):
	musicsql.backend.create_database(**options)
	db = musicsql.backend.alcconnect(**options)
	metadata = MetaData(bind=db)

	Table('files', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('path', Text),
		Column('work_title', Text),
		Column('work_number', Text),
		Column('movement_number', Text),
		Column('movement_title', Text),
		Column('rights', Text),
		Column('encoding_date', Text),
		Column('encoder', Text),
		Column('source', Text),
		Column('divisions', Integer)
		)
	Table('parts', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('file_id', Integer, ForeignKey("files.row_id")),
		Column('part_attr', Text),
		Column('part_name', Text),
		Column('part_group', Text)
		)
	Table('moments', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('file_id', Integer, ForeignKey("files.row_id")),
		Column('measure_id', Integer, index=True),
		Column('ticks', Integer)
		)
	measures = Table('measures', metadata,
					Column('row_id', Integer, primary_key=True),
					Column('file_id', Integer, ForeignKey("files.row_id")),
					Column('number', Integer),
					Column('implicit', String(4)),
					Column('non_controlling', String(4)),
					Column('width', Integer),
					Column('_start_tick', Integer)
					)
	Index('ix_m_nf', measures.c.number, measures.c.file_id)
	events = Table('events', metadata,
				Column('row_id', Integer, primary_key=True),
				Column('file_id', Integer, ForeignKey("files.row_id")),
				Column('part_id', Integer, ForeignKey("parts.row_id")),
				Column('moment_id', Integer, ForeignKey("moments.row_id")),
				Column('attribute_id', Integer, ForeignKey("attributes.row_id")),
				Column('measure_id', Integer, ForeignKey("measures.row_id")),
				Column('beat', Integer)
				)
	Index('ix_e_ps', events.c.part_id, events.c.moment_id, unique=True)
	Table('attributes', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('startevent_id', Integer),
		Column('file_id', Integer),
		Column('_start_tick', Integer),
		Column('fifths', Integer),
		Column('mode', String(16)),
		Column('beats', Integer),
		Column('beat_type', Integer),
		Column('instruments', Text),
		Column('sign', String(16)),
		Column('line', Integer),
		Column('clef_octave_change', Integer),
		Column('staves', Integer),
		Column('directive', Text),
		Column('symbol', Text),
		Column('diatonic', Integer),
		Column('chromatic', Integer),
		Column('double_', Integer)
		)
	Table('notes', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('startevent_id', Integer),
		Column('endevent_id', Integer),
		Column('prevnote_id', Integer),
		Column('prevbeat_id', Integer),
		Column('prevsemit_id', Integer),
		Column('startnotehead_id', Integer),
		Column('concert_step', Integer),
		Column('concert_alter', Integer),
		Column('concert_octave', Integer),
		Column('semit', Integer),
		Column('pc', Integer),
		Column('duration', Integer)
		)
	Table('intersects', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('moment_id', Integer, ForeignKey("moments.row_id")),
		Column('note_id', Integer, ForeignKey("notes.row_id"))
		)
	Table('noteheads', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('note_id', Integer, ForeignKey("notes.row_id")),
		Column('startevent_id', Integer),
		Column('endevent_id', Integer),
		Column('tiedto_id', Integer),
		Column('voice', Integer),
		Column('staff', Integer),
		Column('notehead_duration', Integer),
		Column('actual_notes', Integer),
		Column('normal_notes', Integer),
		Column('rest', Boolean),
		Column('step', String(2)),
		Column('alter_', Float),
		Column('octave', Integer),
		Column('grace_order', Integer),
		Column('steal_time', Integer),
		Column('type', String(8)),
		Column('dot', Integer),
		Column('accidental', String(32)),
		Column('stem', String(8)),
		Column('dynamics', Float),
		Column('articulation', String(32)),
		Column('tie_start', Integer),
		Column('tie_stop', Integer)
		)
	Table('barlines', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('event_id', Integer, ForeignKey("events.row_id")),
		Column('style', String(16)),
		Column('location', String(8))
		)
	Table('slurs', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('startnotehead_id', Integer),
		Column('stopnotehead_id', Integer)
		)
	Table('wedges', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('startevent_id', Integer),
		Column('stopevent_id', Integer),
		Column('type', String(16))
		)
	Table('beams', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('notehead_id', Integer, ForeignKey("noteheads.row_id"), unique=True),
		Column('beam1', String(16)),
		Column('beam2', String(16)),
		Column('beam3', String(16)),
		Column('beam4', String(16)),
		Column('beam5', String(16)),
		Column('beam6', String(16)),
		)
	Table('directions', metadata,
		Column('row_id', Integer, primary_key=True),
		Column('event_id', Integer, ForeignKey("events.row_id")),
		Column('start_tick', Integer),
		Column('type', String(32)),
		Column('value', Text)
		)
	metadata.create_all()
