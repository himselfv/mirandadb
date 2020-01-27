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



"""
Events
"""

# Compares two events, returns their difference mask
def compare_events(db1, db2, e1, e2):
	fail = ""
	if e1.contactID <> e2.contactID:
		fail += "i"
	if db1.get_module_name(e1.ofsModuleName) <> db2.get_module_name(e2.ofsModuleName):
		fail += "m"
	if e1.eventType <> e2.eventType:
		fail += "t"
	if e1.flags <> e2.flags:
		fail += "f"
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
		# It's okay for flags to be different though we would prefer them to match
		# The message could've been read, for example
		# Some flags shouldn't change! These include DBEF_SENT (otherwise "received") and DBEF_RTL.
		# DBEF_UTF CAN change with database upgrades/imports.
		if (fail == "f") and ((e1.flags & (e1.DBEF_SENT+e1.DBEF_RTL)) == (e2.flags & (e2.DBEF_SENT+e2.DBEF_RTL))):
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

# Compares two contacts event by event
def compare_contact_events_print(db1, db2, contact1, contact2, merge=False):
	print ("Comparing "+contact1.display_name+" (#"+str(contact1.contactID)+")"
		+" and "+contact2.display_name+" (#"+str(contact2.contactID)+")...")
	last_db2_event = None	# Keep track of this to quickly insert new ones
	for diff in EventDiffIterator(db1, db2, db1.get_events(contact1), db2.get_events(contact2)):
		if diff.both: last_db2_event = diff.both[-1]
		elif diff.db2: last_db2_event = diff.db2[-1]
		if (not diff.db1) and (not diff.db2):
			continue
		if (diff.db1 == None) and not args.process_new:
			continue
		# Print out ALL events for this timestamp to help figuring out the problem
		for evt in diff.both:
			print "==DB: "+mirandadb.format_event(db2, evt, evt.data)
		for evt in (diff.db1 or []):
			evt.data.size = evt.cbBlob
			print "--DB2: "+mirandadb.format_event(db1, evt, evt.data)
		for evt in diff.db2:
			evt.data.size = evt.cbBlob
			print "++DB2: "+mirandadb.format_event(db2, evt, evt.data)
		if merge and (diff.db1 <> None) and (len(diff.db1) > 0):
			existing_db2 = diff.both + args.db2
			if len(existing_db2) > 0:
				insert_after = existing_db2[-1]		# Insert after the last existing; saves us looking by timestamp
			else:
				insert_after = last_db2_event
			for evt1 in diff.db1:
				# We would have to map event.contactID -> new_event.contactID, but thankfully,
				# contactIDs are the same (they *are* how we map contacts!)
				evt2 = copy.copy(evt1)
				db2.add_event(contact2, evt2)
		print ""	# Empty line


def main():
	parser = argparse.ArgumentParser(description="Compares two snapshots of **the same** Miranda database, looking for changed, added or deleted events.",
		parents=[coreutils.argparser()])
	parser.add_argument("dbname1", help='path to older database file')
	parser.add_argument("dbname2", help='path to newer database file')
	parser.add_argument("--write", help='opens the databases for writing (WARNING: enables editing functions!)', action='store_true')
	parser.add_argument("--contact", type=str, help='diff only this contact')
	parser.add_argument("--process-new", help='processes NEW events in addition to changed or missing events', action='store_true')
	parser.add_argument("--merge-messages", action='store_true', help='imports all missing messages from DB1 into DB2')
	global args
	args = parser.parse_args()
	coreutils.init(args)

	db1 = mirandadb.MirandaDbxMmap(args.dbname1)
	db2 = mirandadb.MirandaDbxMmap(args.dbname2, writeable=args.write)

	if args.contact:
		contacts1 = db1.contacts_by_mask(args.contact)
		contacts2 = db2.contacts_by_mask(args.contact)
	else:
		contacts1 = db1.contacts()
		contacts2 = db2.contacts()

	ret = compare_contact_lists(contacts1, contacts2)
	print "The following contacts from DB1 are missing:"
	for contact1 in ret['missing1']:
		print contact1.display_name
	print "The following contacts in DB2 are new:"
	for contact2 in ret['missing2']:
		print contact2.display_name

	if not args.contact: # explicitly compare one db.user against another
		compare_contact_events_print(db1, db2, db1.user, db2.user, merge=args.merge_messages)
	for (contact1, contact2) in ret['match']:
		compare_contact_events_print(db1, db2, contact1, contact2, merge=args.merge_messages)

if __name__ == "__main__":
	sys.exit(main())
