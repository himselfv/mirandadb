# -*- coding: utf-8 -*-
import sys, os
import argparse
import logging
import coreutils
import mirandadb
import utfutils
import fnmatch

log = logging.getLogger('mirdiff')

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
	for event in db.get_events(contact):
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
		print mirandadb.format_event(db, event, data)

# Compares two contacts event by event
def compare_contacts(db1, db2, contact1, contact2):
	print ("Comparing "+contact1.display_name+" (#"+str(contact1.dwContactID)+")"
		+" and "+contact2.display_name+" (#"+str(contact2.dwContactID)+")...")
	# Events must be time-ordered and are timed with seconds precision.
	# - Start with the beginning.
	# - Skip events on the lesser side until both sides are on the same second [anything missing from one side is missing]
	# - Go over events, event by event
	# - Print any remaining events in the longer chain
	events1 = db1.get_events(contact1)	# this handles metacontacts transparently
	events2 = db2.get_events(contact2)
	
	i1 = 0
	i2 = 0
	while (i1 < len(events1)) or (i2 < len(events2)):
		e1 = events1[i1] if i1 < len(events1) else None
		e2 = events2[i2] if i2 < len(events2) else None

		if (e1 == None) or ((e2 <> None) and (e1.timestamp > e2.timestamp)):
			if args.print_new:
				print "--DB1: " + mirandadb.format_event(db2, e2)
			i2 += 1
			continue
		
		if (e2 == None) or (e2.timestamp > e1.timestamp):
			print "--DB2: " + mirandadb.format_event(db1, e1)
			i1 += 1
			continue
		
		# Collect all events for this second
		timestamp = e1.timestamp
		el1 = [e1]
		el2 = [e2]
		i1 += 1
		while i1 < len(events1):
			e1 = events1[i1]
			if e1.timestamp <> timestamp:
				break
			el1.append(e1)
			i1 += 1
		i2 += 1
		while i2 < len(events2):
			e2 = events2[i2]
			if e2.timestamp <> timestamp:
				break
			el2.append(e2)
			i2 += 1
		
		compare_event_lists(db1, db2, el1, el2)


def compare_find_event(db1, db2, e1, el2):
	for e2 in el2:
		fail = compare_events(db1, db2, e1, e2)
		if fail == "":
			return e2
	return None

# Compares two event lists, tries to find a match for every message
def compare_event_lists(db1, db2, el1, el2):
	el1_missing = []
	el2_all = el2[:]
	for e1 in el1:
		e2 = compare_find_event(db1, db2, e1, el2)
		if e2 <> None:
			el2.remove(e2)
		else:
			# Some events are exact duplicates; if they are missing, we forgive that
			e2 = compare_find_event(db1, db2, e1, el2_all)
		if e2 == None:
			el1_missing.append(e1)
	for e1 in el1_missing:
		print "!-DB2: "+mirandadb.format_event(db1, e1)
	for e2 in el2:
		print "!-DB1: "+mirandadb.format_event(db2, e2)


# Compares two events, returns their difference mask
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
	return fail


def contact_by_id(contacts, id):
	for contact in contacts:
		if contact.dwContactID == id:
			return contact
	return None

# Returns a dict with matched, missing and new contacts
def compare_contact_lists(contacts1 = None, contacts2 = None):
	ret = {}
	ret['match'] = []					# A list of (contact1, contact2) pairs
	ret['missing1'] = contacts1[:]		# Missing from contacts1
	ret['missing2'] = contacts2[:]		# Missing from contacts2
	missing_contact1 = []
	for contact1 in contacts1:
		contact2 = contact_by_id(contacts2, contact1.dwContactID)
		if contact2 <> None:
			ret['match'].append((contact1, contact2))
			ret['missing1'].remove(contact1)
			ret['missing2'].remove(contact2)
	return ret


def main():
	parser = argparse.ArgumentParser(description="Compares two snapshots of **the same** Miranda database, looking for changed, added or deleted events.",
		parents=[coreutils.argparser()])
	parser.add_argument("dbname1", help='path to older database file')
	parser.add_argument("dbname2", help='path to newer database file')
	parser.add_argument("--print-new", help='finds NEW events in addition to changed or missing events', action='store_true')
	parser.add_argument("--contact", help='diff only this contact', type=str)
	global args
	args = parser.parse_args()
	coreutils.init(args)

	db1 = mirandadb.MirandaDbxMmap(args.dbname1)
	db2 = mirandadb.MirandaDbxMmap(args.dbname2)

	if args.contact:
		contacts1 = db1.contacts_by_mask(args.contact)
		contacts2 = db2.contacts_by_mask(args.contact)
	else:
		contacts1 = db1.contacts()
		contacts2 = db2.contacts()

	cmp = compare_contact_lists(contacts1, contacts2)
	print "The following contacts from DB1 are missing:"
	for contact1 in cmp['missing1']:
		print contact1.display_name
	print "The following contacts in DB2 are new:"
	for contact2 in cmp['missing2']:
		print contact2.display_name

	if not args.contact: # explicitly compare one db.user against another
		compare_contacts(db1, db2, db1.user, db2.user)
	for (contact1, contact2) in cmp['match']:
		compare_contacts(db1, db2, contact1, contact2)

if __name__ == "__main__":
	sys.exit(main())
