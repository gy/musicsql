#!/usr/bin/python

import xmlrpclib
import xmlrpccookies

server_url = 'http://localhost/xmlrpc'
parser = xmlrpccookies.CookieTransport('musicsqlserver')
server = xmlrpclib.ServerProxy(server_url, transport=parser)

print server.connect("Bach_SATB")
print server.list_files()
