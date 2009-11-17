#!/usr/bin/python

import xmlrpclib
import xmlrpccookies

server_url = 'http://localhost/xmlrpc'
parser = xmlrpccookies.CookieTransport('musicsqlserver')
server = xmlrpclib.ServerProxy(server_url, transport=parser)

server.connect("Bach_SATB")
part1 = server.part()
note1 = server.part_method(part1, 'add_first_note', '4B4')
part2 = server.part()
moment = server.note_method(note1, 'add_note', '4G3')
note2 = server.moment_method(moment, 'add_note', part2, '4G3')
prevnote1 = server.note_method(note1, 'add_previous_note', 'C5')
prevnote2 = server.note_method(note2, 'add_previous_note', 'F#3')
result = server.run_query()
print result
