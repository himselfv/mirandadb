# -*- coding: utf-8 -*-
import sys, os
import argparse
import logging
import coreutils
import mirandadb
import mirdiff
import utfutils
import fnmatch

log = logging.getLogger('mirrestore')

# Enhances MirandaDbxMmap with some data scan/restore capabilities
class MirandaDbxMmapChk(mirandadb.MirandaDbxMmap):
	def utf8trydecode(self, data):
		ret = super(MirandaDbxMmapChk, self).utf8trydecode(data)
		if hasattr(ret, 'problem'):
			return ret
		
		# Verify that the text looks like valid UTF16 after decoding
		test = utfutils.utf16test(ret.text)
		if test == True:
			return ret
		ret.problem = test
		ret.utf8 = data.encode('hex')
		text_bytes = utfutils.utf16bytes(ret.text)
		ret.utf16 = text_bytes.encode('hex')
		ret.text = None # remove text to indicate problems
		
		"""
		# There are some cases where DECODED utf16 contains utf8!
		# Let's try to analyze this
		try:
			# This may again end with \0
			text_bytes = utfutils.removeterm0(text_bytes)
			text2 = text2_bytes.decode('utf-8')
		except UnicodeDecodeError as err:
			text2 = "Doubly decode failed: "+str(err)
		else:
			ret2 = utfutils.utf16test(text2)
			if ret2 == True:
				return (False, text2, 'Doubly encoded utf8!')
		# Doesn't seem to be the case; just return the original attempt
		"""
		return ret

def dump_events(args):
	db = MirandaDbxMmapChk(args.dbname)
	bad_event_count = 0
	bad_offsets = {}
	for contact in mirandadb.select_contacts_opt(db, args.contact):
		print "Events for "+contact.display_name+": "
		for event in db.get_events(contact):
			data = event.data
			if hasattr(data, 'problem'):
				bad_event_count += 1
			if args.bad and not hasattr(data, 'problem'):
				continue
			if args.bad_offsets:
				data.offset = event.offset
				bad_offset = event.offset // 0x10000
				if bad_offset in bad_offsets:
					bad_offsets[bad_offset] += 1
				else:
					bad_offsets[bad_offset] = 1
			print mirandadb.format_event(db, event, data)
	log.warning("Bad events: "+str(bad_event_count))
	print "Bad events:"+str(bad_event_count)
	if args.bad_offsets:
		print "Bad offsets:"
		print '\n'.join([ repr(key) + ': ' + repr(value) for (key, value) in bad_offsets.items()])


"""
Tries to fix corrupted messages by importing messages from older, non-corrutped database.
1. Imports all old DB messages missing from the newer DB. (== 'mirdiff merge')
2. Deletes new DB messages which:
   - are missing from the older DB
   - have likely prototypes in the older DB

So for example. Here's a typical situation *for a specific timestamp*:
DB_old:
  Message1
  Message1  [a duplicate]
  Message2
  Message3
DB_new:
  Message1
  Message2
  Message2  [a duplicate]
  CorruptedMessage   [may be Message3, or another duplicate of Message1 or Message2]

Message3 will be imported.
If any of [Message1, Message2, Message3] matching CorruptedMessage module, type and flags, CorruptedMessage will be deleted.

This doesn't analyze whether CorruptedMessage is in fact corrupt. Too hard to tell.
"""
def restore_events(args):
	db1 = mirandadb.MirandaDbxMmap(args.old_dbname)
	db2 = mirandadb.MirandaDbxMmap(args.dbname, writeable=args.write)
	contacts1 = mirandadb.select_contacts_opt(db1, args.contact)
	contacts2 = mirandadb.select_contacts_opt(db2, args.contact)
	contacts_map = mirdiff.compare_contact_lists(contacts1, contacts2)
	for (contact1, contact2) in contacts_map['match']:
		restore_events_contact(db1, db2, contact1, contact2)

# Compares two contacts event by event
def restore_events_contact(db1, db2, contact1, contact2):
	print ("Restoring "+contact1.display_name+" (#"+str(contact1.contactID)+")"
		+" to "+contact2.display_name+" (#"+str(contact2.contactID)+")...")
	last_db2_event = None
	for diff in mirdiff.EventDiffIterator(db1, db2, db1.get_events(contact1), db2.get_events(contact2)):
		if diff.both: last_db2_event = diff.both[-1]
		elif diff.db2: last_db2_event = diff.db2[-1]
		if (not diff.db1) and (not diff.db2):
			continue
		if diff.db1 == None:
			continue			# Ignore entirely new events

		if args.print_diff:
			mirdiff.print_event_diff(db1, db2, diff)

		# Import events missing from DB2
#		for evt1 in diff.db1:
#			print "Importing event "+str(evt1.offset)+" as missing"
#			last_db2_event = mirdiff.import_event(db1, db2, contact2, evt1, last_db2_event)
		
		# Delete all DB2-only events which match something in DB1 at least by module, type, contact and flags
		for e2 in diff.db2:
			e1_match = None
			for e1 in diff.db1:
				fail = mirdiff.compare_events(db1, db2, e1, e2).replace('b','').replace('f','')
				if fail == '':
					e1_match = e1
					break
				print fail
			for e1 in diff.both: # These are in fact db2 events
				fail = mirdiff.compare_events(db2, db2, e1, e2).replace('b','').replace('f','')
				if fail == '':
					e1_match = e1
					break
				print fail
			if e1_match <> None:
				print "Deleting event "+str(e2.offset)+', matches DB1 event '+str(e1_match.offset)
				db2.delete_event(e2)
			else:
				print "Skipping DB2-only event "+str(e2.offset)+', no match in DB1'


# Main
parser = argparse.ArgumentParser(description="Analyzes Miranda database for corruption.",
	parents=[coreutils.argparser()])
parser.add_argument("dbname", help='path to database file')
parser.add_argument("--write", help='opens the databases for writing (WARNING: enables editing functions!)', action='store_true')
subparsers = parser.add_subparsers(title='subcommands')

sparser = subparsers.add_parser('dump-events', help='prints all events for the given contacts')
sparser.add_argument('contact', type=str, nargs='*', help='print events for these contacts')
sparser.add_argument("--bad", help='dumps only bad events', action='store_true')
sparser.add_argument("--bad-offsets", help='gathers bad event offset statistics', action='store_true')
sparser.set_defaults(func=dump_events)

sparser = subparsers.add_parser('restore-events', help='restores events which differ from another DB')
sparser.add_argument('--contact', type=str, nargs='*', help='restore events for these contacts')
sparser.add_argument('--old-dbname', type=str, required=True, help='use this old db version')
sparser.add_argument('--print-diff', action='store_true', help='print diff in addition to addition/deletion messages')
sparser.set_defaults(func=restore_events)

args = parser.parse_args()
coreutils.init(args)
	
if args.func <> None:
	args.func(args)
