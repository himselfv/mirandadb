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
Deletes new DB messages which:
 - are missing from the older DB
 - have likely prototypes in the current DB
For this to work, import uncorrputed versions from the older DB with `mirdiff --merge` first.

Example. Here's a typical situation *for a specific timestamp*:
DB_old:
  Message1
  Message1  [a duplicate]
  Message2
  Message3
DB_new:
  Message1
  Message2
  Message2  [a duplicate]
  CorruptedMessage		[missing from DB_old]

If any of [Message1, Message2] matches CorruptedMessage module, type and flags, CorruptedMessage will be deleted.
This doesn't analyze whether CorruptedMessage is in fact corrupt. Too hard to tell.
"""
def delete_extra_events(args):
	db1 = mirandadb.MirandaDbxMmap(args.old_dbname)
	db2 = mirandadb.MirandaDbxMmap(args.dbname, writeable=args.write)
	contacts1 = mirandadb.select_contacts_opt(db1, args.contact)
	contacts2 = mirandadb.select_contacts_opt(db2, args.contact)
	contacts_map = mirdiff.compare_contact_lists(contacts1, contacts2)
	for (contact1, contact2) in contacts_map['match']:
		delete_extra_events_contact(db1, db2, contact1, contact2)

# Compares two contacts event by event
def delete_extra_events_contact(db1, db2, contact1, contact2):
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
		
		# Delete all DB2-only events which match something in DB1 at least by module, type, contact and flags
		for e2 in diff.db2:
			e1_match = None
			for e1 in diff.db1:
				fail = mirdiff.compare_events(db1, db2, e1, e2).replace('b','').replace('f','')
				if fail == '':
					e1_match = e1
					break
			for e1 in diff.both: # These are in fact db2 events
				fail = mirdiff.compare_events(db2, db2, e1, e2).replace('b','').replace('f','')
				if fail == '':
					e1_match = e1
					break
			if e1_match <> None:
				print "Deleting: "+mirandadb.format_event(db, event, data)
				print "Matches: "+str(e1_match.offset)
				db2.delete_event(e2)
			else:
				print "Skipping: "+mirandadb.format_event(db, event, data)


# Main
parser = argparse.ArgumentParser(description="Analyzes Miranda database for corruption.",
	parents=[coreutils.argparser()])
parser.add_argument("dbname", help='path to database file')
parser.add_argument("--write", help='opens the databases for writing (WARNING: enables editing functions!)', action='store_true')
subparsers = parser.add_subparsers(title='subcommands')

sparser = subparsers.add_parser('dump-events', help='prints all events for the given contacts',
	description="""
		Analyzes unicode composition of the messages and tries to determine which messages seem corrupted.
		Highly unscientific, because:
		  1. What's rare for one language is common in another.
		  2. Corrupted messages sometimes take any forms, including chunks of unrelated latin texts, so look entirely bening.
		If you have a non-corrupted version of the database you may be better off with `mirdiff`.
	""")
sparser.add_argument('contact', type=str, nargs='*', help='print events for these contacts')
sparser.add_argument("--bad", help='dumps only bad events', action='store_true')
sparser.add_argument("--bad-offsets", help='gathers bad event offset statistics', action='store_true')
sparser.set_defaults(func=dump_events)

sparser = subparsers.add_parser('delete-extra', formatter_class=coreutils.SmartFormatter,
	help='delete messages which are missing from the older version of the database',
	description="""
		Compares the database to its older version and deletes all events that are:
		  1. Not entirely new (== in the timespan that the databases share)
		  2. Are missing from the older version of the DB
		  3. There are messages for the same timespan which **may** be their uncorrupted version.
		This is commonly used to repair corrupted message bodies:
		  1. `mirdiff --merge` older database into newer one (bringing clean copies of all corrupted messages)
		  2. `delete-extra` to remove corrupted copies (as they are not in older DB)
		Note that this **does not check that messages are in fact corrupted**.
	""")
sparser.add_argument('--contact', type=str, nargs='*', help='delete events for these contacts')
sparser.add_argument('--old-dbname', type=str, required=True, help='use this old db version')
sparser.add_argument('--print-diff', action='store_true', help='print event differences between versions')
sparser.set_defaults(func=delete_extra_events)

args = parser.parse_args()
coreutils.init(args)
	
if args.func <> None:
	args.func(args)
