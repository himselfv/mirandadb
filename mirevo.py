# -*- coding: utf-8 -*-
import sys, os
import argparse
import logging
import coreutils
import mirandadb
import utfutils
import fnmatch

log = logging.getLogger('mirevo')

class ContactHistory(object):
	def __init__(self, contactId):
		self.contactId = contactId
		self.props = {}
		
	def add_prop(self, version, propName, value):
		if propName in self.props:
			versions = self.props[propName]
		else:
			versions = []
		if (len(versions) > 0) and (versions[-1][1] == value):
			return
		versions.append((version, value))
		self.props[propName] = versions


# Scans another database and adds contact history entries
def contact_evo_scan(db, contact_histories):
	ver = os.path.basename(db.filename)
	for contact in db.contacts():
		contact_history = contact_histories.get(contact.dwContactID, None)
		if contact_history == None:
			contact_history = ContactHistory(contact.dwContactID)
		# Add all properties which we track
		contact_history.add_prop(ver, 'id', contact.id)
		contact_history.add_prop(ver, 'nick', contact.nick)
		contact_history.add_prop(ver, 'display_name', contact.display_name)
		contact_histories[contact.dwContactID] = contact_history

# Prints one contact history
def contact_evo_print(contact_history):
	print "#"+str(contact_history.contactId)
	for prop in contact_history.props:
		revs = contact_history.props[prop]
		for rev in revs:
			print rev[0]+u"\\"+prop + u": " + unicode(rev[1])
	print ""


# Main
parser = argparse.ArgumentParser(description="Loads all matching database snapshots one by one and traces data evolution through it.",
	parents=[coreutils.argparser()])
parser.add_argument("mask", help='path and file mask for the database files')
parser.add_argument("--contacts", help='trace the evolution of contact properties', action='store_true')
args = parser.parse_args()
coreutils.init(args)

# Parse mask
(dir, mask) = os.path.split(args.mask)
if dir == '':
	dir = '.'

# Find all and order by modification time
files = []
for filename in fnmatch.filter(os.listdir(dir), mask):
	fname = dir+'\\'+filename
	files.append((os.path.getmtime(fname), fname))
files.sort()

# Zero vars
contact_histories = {}	# id -> contact

# Process
for file in files:
	log.info("Processing "+file[1]+"...")
	db = mirandadb.MirandaDbxMmap(file[1])
	if args.contacts:
		contact_evo_scan(db, contact_histories)

if args.contacts:
	print "Contacts:"
	for contactId in contact_histories:
		contact_evo_print(contact_histories[contactId])
