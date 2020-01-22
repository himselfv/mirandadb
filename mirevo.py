# -*- coding: utf-8 -*-
import sys, os
import argparse
import logging
import coreutils
import mirandadb
import utfutils
import fnmatch
import datetime

log = logging.getLogger('mirevo')

# Returns the text to identify db in the version list (configurable)
def db_get_version(db):
	fname = db.filename
	if args.version_by == 'filename':
		return os.path.basename(fname)
	else:
		return datetime.datetime.fromtimestamp(os.path.getmtime(fname)).strftime('%Y.%m.%d')

class ContactHistory(object):
	def __init__(self, contactId):
		self.contactId = contactId
		self.props = {}
		self.props_by_ver = {}
	
	def add_prop(self, version, propName, value):
		if propName in self.props:
			versions = self.props[propName]
		else:
			versions = []
		if (len(versions) > 0) and (versions[-1][1] == value):
			return	# Do not add neither this nor props_by_version
		versions.append((version, value))
		self.props[propName] = versions
		
		if version in self.props_by_ver:
			props = self.props_by_ver[version]
		else:
			props = []
		props.append((propName, value))
		self.props_by_ver[version] = props
	
	def prop_has_changes(self, propName):
		return (propName in self.props) and (len(self.props[propName]) > 1)

# Scans another database and adds contact history entries
def contact_evo_update(contact_histories, db):
	ver = db_get_version(db)
	for contact in db.contacts():
		contact_history = contact_histories.get(contact.contactID, None)
		if contact_history == None:
			contact_history = ContactHistory(contact.contactID)
		# Add all properties which we track
		if not args.all_props:
			contact_history.add_prop(ver, 'id', contact.id)
			contact_history.add_prop(ver, 'nick', contact.nick)
			contact_history.add_prop(ver, 'display_name', contact.display_name)
		else:
			for moduleName in contact.settings:
				module = contact.settings[moduleName]
				for setting in module:
					contact_history.add_prop(ver, moduleName+'\\'+setting.name, setting.value)
			
		contact_histories[contact.contactID] = contact_history

# Prints one contact history
def contact_evo_print(contact_history):
	print "#"+str(contact_history.contactId)
	if args.group_by == 'prop':
		for prop in contact_history.props:
			revs = contact_history.props[prop]
			if args.only_changes and (len(revs) <= 1):
				continue
			for rev in revs:
				print rev[0]+u"\t"+prop + u"\t\t" + unicode(rev[1])
	else:
		# Sort by version
		props2 = sorted([(ver, contact_history.props_by_ver[ver]) for ver in contact_history.props_by_ver])
		for (ver, revs) in props2:
			for rev in revs:
				if args.only_changes and not contact_history.prop_has_changes(rev[0]):
					continue
				print ver+u"\t"+rev[0] + u"\t\t" + unicode(rev[1])
	print ""


# Main
parser = argparse.ArgumentParser(description="Loads all matching database snapshots one by one and traces data evolution through it.",
	parents=[coreutils.argparser()])
parser.add_argument("mask", help='path and file mask for the database files')
parser.add_argument("--contacts", help='trace the evolution of contact properties', action='store_true')
parser.add_argument("--only-changes", help='skip properties which have exactly one version', action='store_true')
parser.add_argument("--sort-by", help='order input files by', choices=['filename', 'modified'], default='modified' )
parser.add_argument("--version-by", help='what to use as a version identifier', choices=['filename', 'modified'], default='modified' )
parser.add_argument("--group-by", help='group the results by', choices=['prop', 'ver'], default='prop' )
parser.add_argument("--all-props", help='scan all database settings instead of the chosen few', action='store_true')
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
	fmtime = os.path.getmtime(fname)
	if args.sort_by == 'filename':
		key = fname
	else:
		key = fmtime
	files.append((key, fname, fmtime))
files.sort()	# by first entry, the key

# Zero vars
contact_histories = {}	# id -> contact

# Process
for file in files:
	log.info("Processing "+file[1]+"...")
	db = mirandadb.MirandaDbxMmap(file[1])
	if args.contacts:
		contact_evo_update(contact_histories, db)

if args.contacts:
	print "Contacts:"
	for contactId in contact_histories:
		contact_evo_print(contact_histories[contactId])
