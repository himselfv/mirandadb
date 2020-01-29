# -*- coding: utf-8 -*-
import sys, os
import argparse
import logging
import coreutils
import mirandadb
import utfutils
import fnmatch
import __builtin__
import copy

log = logging.getLogger('mirdiff')


"""
Modules
"""
# Maps DB1 module offsets to DB2 module offsets for the same modules:
#   DB1_offset	-> DB2_offset / None
#   None		-> [DB2_offset, DB2_offset...]
def map_modules(db1, db2):
	ret = {}
	ret[None] = []
	for module1 in db1.get_modules():
		ret[module1.offset] = db2.find_module_name(module1.name)
	for module2 in db2.get_modules():
		if not module2.offset in ret.values():
			ret[None].append(module2.offset)
	return ret

def print_modules_diff(db1, db2, diff):
	missing = [offset for offset in diff.keys() if (offset<>None) and (diff[offset]==None)]
	new = diff[None]
	for offset in missing:
		moduleNane = db1.get_module_name(offset)
		print "--DB2: "+moduleName
		if args.merge_modules:
			new_offset = db2.add_module_name(moduleName)
	for offset in new:
		print "++DB2: "+db2.get_module_name(offset)


"""
Contacts
"""

def contact_by_id(contacts, id):
	for contact in contacts:
		if contact.contactID == id:
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
		contact2 = contact_by_id(contacts2, contact1.contactID)
		if contact2 <> None:
			ret['match'].append((contact1, contact2))
			ret['missing1'].remove(contact1)
			ret['missing2'].remove(contact2)
	return ret

# Maps DBContacts from list1 to DBContacts from list2:
# Lists can be from different DBs. Pass DBs instead of lists for better matching.
#   L1_contact	-> L2_contact / None
#   None		-> [L2_contact, L2_contact...]
# ATM only maps by contact ID but don't rely on that.
def map_contacts(list1, list2):
	if isinstance(list1, mirandadb.MirandaDbxMmap):
		list1 = list1.get_contacts()[:]
	if isinstance(list2, mirandadb.MirandaDbxMmap):
		list2 = list2.get_contacts()[:]
	ret = {}
	ret[None] = []
	for contact1 in list1:
		contact2 = contact_by_id(list2, contact1.contactID)
		ret[contact1] = contact2
	for contact2 in list2:
		if not contact2 in ret.values():
			ret[None].append(contact2)
	return ret

# Same, but returns ID->ID map
def map_contact_ids(list1, list2):
	ret = {}
	ret[None] = []
	for contact1 in map_contacts(list1, list2):
		val = list1[contact1]
		if contact1 <> None:
			ret[contact1.contactID] = val.contactID if val <> None else None
			continue
		for contact2 in val:
			ret[None].append(contact2.contactID)
	return ret


"""
Events
"""

# Compares two events, returns their difference mask
def compare_events(db1, db2, e1, e2):
	fail = ""
	if e1.contactID <> e2.contactID:
		fail += "i"
	if db1.get_module_name(e1.ofsModuleName) <> db2.get_module_name(e2.ofsModuleName):
		print db1.get_module_name(e1.ofsModuleName)
		print e1.ofsModuleName
		print db2.get_module_name(e2.ofsModuleName)
		print e2.ofsModuleName
		fail += "m"
	if e1.eventType <> e2.eventType:
		fail += "t"
	if e1.flags <> e2.flags:
		# Some flags are less permanent than others, e.g. DBEF_READ.
		# Permanent flags are: DBEF_SENT (==outgoing) and DBEF_RTL
		# DBEF_UTF CAN change with database upgrades/imports.
		if (e1.flags & (e1.DBEF_SENT+e1.DBEF_RTL)) == (e2.flags & (e2.DBEF_SENT+e2.DBEF_RTL)):
			fail += "f"
		else:
			fail += "F"
	if hasattr(e1, 'data') and hasattr(e2, 'data') and (getattr(e1.data, 'text', -1) == getattr(e2.data, 'text', -2)):
		# Some events may have changed from ASCII to Unicode, that's okay as long as text is the same
		pass
	elif e1.blob <> e2.blob:
		fail += "b"
	return fail


# Event comparison results for two event lists (usually all events from each DB for a given timestamp)
class EventDiff:
	def __init__(self, both = None, db1 = None, db2 = None):
		self.both = both		# Messages from db2 which have exact matches in db1
		self.db1 = db1			# Messages from db1 which do not have matches in db2, or []
								# "None" if db1 event chain ENDS before these events from db2. (Usually means that db2 events are simply NEWER)
		self.db2 = db2			# --//--

# Compares two event lists, tries to find a match for every message
# Returns EventDiff
def compare_event_lists(db1, db2, el1, el2):
	diff = EventDiff(both=[], db1=[])
	diff.db2 = el2[:]	# Start with all of them as new
	for e1 in el1:
		e2 = compare_find_event(db1, db2, e1, diff.db2)
		if e2 <> None:
			diff.db2.remove(e2)
		else:
			# Try in already matched e2 events. Some events are exact duplicates, we forgive if those go missing.
			e2 = compare_find_event(db1, db2, e1, diff.both)
		if e2 == None:
			diff.db1.append(e1)
		else:
			diff.both.append(e2)
	# Scan e2 remainder for exact duplicates on e1
	for e2 in diff.db2[:]:
		e1 = compare_find_event(db2, db1, e2, el1)	# in untocuhed el1 because we allow duplicates
		if e1 <> None:
			diff.db2.remove(e2)
			diff.both.append(e2)
	return diff

# Locates the event e1 from db1 in the list of events el2 from db2
def compare_find_event(db1, db2, e1, el2):
	f_candidates = []
	for e2 in el2:
		fail = compare_events(db1, db2, e1, e2)
		if fail == "":
			return e2
		if fail == "f":
			f_candidates.append(e2)
	if len(f_candidates) > 0:
		return f_candidates[0]
	return None


# Given two event iterators, compares them timestamp-by-timestamp and produces EventDiff()s for each timestamp
# * Requires events to be ordered by timestamp, as they normally are.
# * Your iterators need to merge/split metacontacts transparently if you want to ignore metacontact event reparenting.
class EventDiffIterator:
	# Events must be time-ordered and are timed with seconds precision.
	# - Start with the beginning.
	# - Skip events on the lesser side until both sides are on the same second [anything missing from one side is missing]
	# - Go over events, event by event
	# - Print any remaining events in the longer chain
	def __init__(self, db1, db2, events1, events2):
		self.db1 = db1
		self.db2 = db2
		self.events1 = iter(events1)
		self.events2 = iter(events2)
		self.e1 = __builtin__.next(self.events1, None)
		self.e2 = __builtin__.next(self.events2, None)
	def __iter__(self):
		return self
	def next(self):
		while True:
			if (self.e1 == None) and (self.e2 == None):
				raise StopIteration()
			
			# No more db1 events
			if self.e1 == None:
				diff = EventDiff(both=[], db1=None, db2=[self.e2])
				self.e2 = __builtin__.next(self.events2, None)
				return diff
			
			# No more db2 events
			if self.e2 == None:
				diff = EventDiff(both=[], db1=[self.e1], db2=None)
				self.e1 = __builtin__.next(self.events1, None)
				return diff
			
			# Collect all events for the lowest of two timestamps
			if self.e2.timestamp >= self.e1.timestamp:
				timestamp = self.e1.timestamp
			else:
				timestamp = self.e2.timestamp
			
			el1 = []
			el2 = []
			while (self.e1 <> None) and (self.e1.timestamp == timestamp):
				el1.append(self.e1)
				self.e1 = __builtin__.next(self.events1, None)
			while (self.e2 <> None) and (self.e2.timestamp == timestamp):
				el2.append(self.e2)
				self.e2 = __builtin__.next(self.events2, None)

			diff = compare_event_lists(self.db1, self.db2, el1, el2)
			diff.timestamp = timestamp
			return diff

def compare_contact_events(db1, db2, contact1, contact2):
	return EventDiffIterator(db1, db2, db1.get_events(contact1), db2.get_events(contact2))


# We want to insert new events after the LAST EVENT FOR THEIR TIMESTAMP
# The event chain might contain events for other contacts which we haven't even considered:
#    c1@10 -> c1@10 -> c2@10 -> [want to insert here] -> c1@11
# We have to start with any event with the timestamp <= required, and scan forward
def find_event_insert_point(db, contact, timestamp, start_event):
	if start_event == None:
		if contact.ofsFirstEvent == 0:
			return None
		start_event = db.read_event(contact.ofsFirstEvent)
	while start_event.ofsNext > 0:
		next_event = db.read_event(start_event.ofsNext)
		if next_event.timestamp >= timestamp:
			break
		start_event = next_event
	return start_event

# Imports event evt1 from foreign DB1 to DB2. Returns its offset.
def import_event(db1, db2, contact2, evt1, insert_after):
	# Find insert point
	insert_after = find_event_insert_point(db2, contact2, evt1.timestamp+1, insert_after)
	# Convert DB1 event to DB2 event
	evt2 = copy.copy(evt1)
	# We would have to map event.contactID -> new_event.contactID,
	# but thankfully we *match* contacts by IDs so they are by definition equal
	evt2.contactID = evt1.contactID
	# Module's offset might've changed - this happens in the wild
	# Note: Preserve the original event module name, even if the contact protocol have changed
	evt2.ofsModuleName = db2.find_module_name(db1.get_module_name(evt1.ofsModuleName))
	assert(evt2.ofsModuleName <> None)
	return db2.add_event(evt2, contact2, insert_after=insert_after)
	

def print_event_diff(db1, db2, diff):
	# Print out ALL events for this timestamp to help figuring out the problem
	for evt in diff.both:
		print "==DB: "+mirandadb.format_event(db2, evt, evt.data)
	for evt in (diff.db1 or []):
		print "--DB2: "+mirandadb.format_event(db1, evt, evt.data)
	for evt in diff.db2:
		print "++DB2: "+mirandadb.format_event(db2, evt, evt.data)

# Compares two contacts event by event
def compare_contact_events_print(db1, db2, contact1, contact2, merge=False):
	print ("Comparing "+contact1.display_name+" (#"+str(contact1.contactID)+")"
		+" and "+contact2.display_name+" (#"+str(contact2.contactID)+")...")
	last_db2_event = None	# Keep track to quickly insert new ones
	for diff in EventDiffIterator(db1, db2, db1.get_events(contact1), db2.get_events(contact2)):
		if diff.both: last_db2_event = diff.both[-1]
		elif diff.db2: last_db2_event = diff.db2[-1]
		if (not diff.db1) and (not diff.db2):
			continue
		if (diff.db1 == None) and not args.process_new:
			continue
		if diff.db2 == None: diff.db2 = []	# we don't care about particulars with DB2
		print_event_diff(db1, db2, diff)
		if merge and (diff.db1 <> None):
			for evt1 in diff.db1:
				last_db2_event = import_event(db1, db2, contact2, evt1, last_db2_event)
		print ""	# Empty line


"""
main
"""
def main():
	parser = argparse.ArgumentParser(description="Compares two snapshots of **the same** Miranda database, looking for changed, added or deleted events.",
		parents=[coreutils.argparser()])
	parser.add_argument("dbname1", help='path to older database file')
	parser.add_argument("dbname2", help='path to newer database file')
	parser.add_argument("--write", help='opens the databases for writing (WARNING: enables editing functions!)', action='store_true')
	parser.add_argument("--contact", type=str, nargs='*', help='diff only this contact')
	parser.add_argument("--modules", action='store_true', help='diff/merge modules')
	parser.add_argument("--contacts", action='store_true', help='diff/merge contacts')
	parser.add_argument("--events", action='store_true', help='diff/merge events')
	
	parser.add_argument("--process-new", help='process NEW events in addition to changed or missing events', action='store_true')
	parser.add_argument("--merge-modules", action='store_true', help='imports all missing modules from DB1 into DB2')
	parser.add_argument("--merge-messages", action='store_true', help='imports all missing messages from DB1 into DB2')
	global args
	args = parser.parse_args()
	coreutils.init(args)

	db1 = mirandadb.MirandaDbxMmap(args.dbname1)
	db2 = mirandadb.MirandaDbxMmap(args.dbname2, writeable=args.write)

	global modules_map
	modules_map = map_modules(db1, db2)
	if args.modules:
		print "Modules:"
		print_modules_diff(db1, db2, modules_map)

	global contacts_map
	contacts1 = mirandadb.select_contacts_opt(db1, args.contact)
	contacts2 = mirandadb.select_contacts_opt(db2, args.contact)
	contacts_map = compare_contact_lists(contacts1, contacts2)
	if args.contacts:
		print "Contacts:"
		for contact1 in contacts_map['missing1']:
			print "--DB2: "+contact1.display_name+' (#'+str(contact1.contactID)+')'
		for contact2 in contacts_map['missing2']:
			print "++DB2: "+contact2.display_name+' (#'+str(contact2.contactID)+')'

	if args.events:
		if not args.contact: # explicitly compare one db.user against another
			compare_contact_events_print(db1, db2, db1.user, db2.user, merge=args.merge_messages)
		for (contact1, contact2) in contacts_map['match']:
			compare_contact_events_print(db1, db2, contact1, contact2, merge=args.merge_messages)

if __name__ == "__main__":
	sys.exit(main())
