# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import coreutils
import mirandadb
import utfutils

log = logging.getLogger('miranda-dbx_mmap')

# Enhances MirandaDbxMmap with some data scan/restore capabilities
class MirandaDbxMmapChk(mirandadb.MirandaDbxMmap):
	def utf8trydecode(self, data):
		ret = super(MirandaDbxMmapChk, self).utf8trydecode(data)
		if 'problem' in ret:
			return ret
		
		# Verify that the text looks like valid UTF16 after decoding
		test = utfutils.utf16test(ret['text'])
		if test == True:
			return ret
		ret['problem'] = test
		ret['utf8'] = data.encode('hex')
		text_bytes = utfutils.utf16bytes(ret['text'])
		ret['utf16'] = text_bytes.encode('hex')
		ret['text'] = None # remove text to indicate problems
		
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


bad_event_count = 0
bad_offsets = {}		# Bad event offset statistics

def dump_events(db, contact):
	print "Events for "+contact.display_name+": "
	global bad_event_count
	global bad_offsets
	ofsEvent = contact.ofsFirstEvent
	while ofsEvent <> 0:
		event = db.read(mirandadb.DBEvent(), ofsEvent)
		ofsEvent = event.ofsNext
		data = db.decode_event_data(event)
		if isinstance(data, dict) and ('problem' in data):
			bad_event_count += 1
		if args.bad_events:
			if not isinstance(data, dict):
				continue
			if not ('problem' in data):
				continue
		if args.bad_offsets:
			data['offset'] = event.offset
			bad_offset = event.offset // 0x10000
			if bad_offset in bad_offsets:
				bad_offsets[bad_offset] += 1
			else:
				bad_offsets[bad_offset] = 1
		print mirandadb.format_event(event, data)


def compare(db, old_db_filename):
	old_db = MirandaDbxMmapChk(old_db_filename)
	
	contacts1 = old_db.contacts()
	contacts2 = db.contacts()
	missing_contacts1 = []
	
	compare_contacts(old_db, db, old_db.user, db.user)
	for contact1 in contacts1:
		contact2 = db.contact_by_id(contact1.dwContactID)
		if contact2 <> None:
			contacts2.remove(contact2)
			compare_contacts(old_db, db, contact1, contact2)
		else:
			missing_contacts1.append(contact1)
	
	print "The following contacts from DB1 are missing:"
	for contact1 in missing_contacts1:
		print contact1.display_name
	print "The following contacts in DB2 are new:"
	for contact2 in contacts2:
		print contact2.display_name

# Compares two contacts event by event
def compare_contacts(db1, db2, contact1, contact2):
	print "Comparing "+contact1.display_name+" and "+contact2.display_name+"..."
	# Events must be time-ordered and are timed with seconds precision.
	# - Start with the beginning.
	# - Skip events on the lesser side until both sides are on the same second [anything missing from one side is missing]
	# - Go over events, event by event
	# - Print any remaining events in the longer chain
	e1offset = contact1.ofsFirstEvent
	e2offset = contact2.ofsFirstEvent
	while (e1offset <> 0) or (e2offset <> 0):
		e1 = db1.read_event(e1offset) if e1offset <> 0 else None
		e2 = db2.read_event(e2offset) if e2offset <> 0 else None

		if (e1 == None) or ((e2 <> None) and (e1.timestamp > e2.timestamp)):
			if args.compare_print_new:
				print "--DB1: " + mirandadb.format_event(db2, e2)
			e2offset = e2.ofsNext
			continue
		
		if (e2 == None) or (e2.timestamp > e1.timestamp):
			print "--DB2: " + mirandadb.format_event(db1, e1)
			e1offset = e1.ofsNext
			continue
		
		compare_events(db1, db2, e1, e2)
		e1offset = e1.ofsNext
		e2offset = e2.ofsNext

def compare_events(db1, db2, e1, e2):
	fail = ""
	if e1.contactID <> e2.contactID:
		fail += "i"
	if db1.get_module_name(e1.ofsModuleName) <> db2.get_module_name(e2.ofsModuleName):
		fail += "m"
	if e1.flags <> e2.flags:
		fail += "f"
	if e1.eventType <> e2.eventType:
		fail += "t"
	if e1.blob <> e2.blob:
		fail += "b"
	if fail:
		print "!=DB1: "+mirandadb.format_event(db1, e1)
		print "!=DB2: "+mirandadb.format_event(db2, e2)
		print fail

# Main
parser = argparse.ArgumentParser(description="Parse and print Miranda.",
	parents=[coreutils.argparser()])
parser.add_argument("dbname", help='path to database file')
parser.add_argument("--dump-events", help='prints all events for the given contact', metavar='contact_mask', type=str, action='append')
parser.add_argument("--bad-events", help='dumps bad events only', action='store_true')
parser.add_argument("--bad-offsets", help='gathers bad event offset statistics', action='store_true')
parser.add_argument("--compare", help='compares two copies of **the same** Miranda database, looking for changed events', metavar='dbname', type=str)
parser.add_argument("--compare-print-new", help='finds NEW events in addition to changed or missing events', action='store_true')
args = parser.parse_args()
coreutils.init(args)

db = MirandaDbxMmapChk(args.dbname)

if args.dump_events:
	for contact_name in args.dump_events:
		for contact in db.contacts_by_mask(contact_name):
			dump_events(db, contact)
	log.warning("Bad events: "+str(bad_event_count))
	print "Bad events:"+str(bad_event_count)
	if args.bad_offsets:
		print "Bad offsets:"
		print '\n'.join([ repr(key) + ': ' + repr(value) for (key, value) in bad_offsets.items()])

if args.compare:
	compare(db, args.compare)