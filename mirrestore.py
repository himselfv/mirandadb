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
Verifies the database integrity
"""
def vassert(condition, message):
	if not condition:
		print "WARNING: "+message

class DbVerifier(mirandadb.MirandaDbxMmap):
	def verify(self):
		header = self.header
		self.totalUsed = header.size()
	
		# Header
		vassert(header.ofsModuleNames <> 0, '0 modules')
		vassert(header.ofsUser <> 0, 'No self contact')
		vassert(header.ofsFirstContact <> 0, '0 contacts')
	
		# Modules
		offset = header.ofsModuleNames
		self.moduleOffsets = []
		while offset <> 0:
			self.moduleOffsets.append(offset)
			module = self.read_module(offset)	# raises on bad offset/signature
			offset = module.ofsNext
			self.totalUsed += module.size()
	
		# Contacts
		self.verify_contacts()
		
		self.file.seek(0, 2)
		self.fileSize = self.file.tell()
		vassert(self.fileSize-self.totalUsed == self.header.slackSpace,
			'FileSize:'+str(self.fileSize)+' - TotalUsed:'+str(self.totalUsed)+' != SlackSpace:'+str(self.header.slackSpace)+' (diff='
			+str(self.fileSize-self.totalUsed-self.header.slackSpace)+')'
			)
	
	def verify_contacts(self):
		contactCount = 0
		self.contactIDs = {}
		
		self.verify_contact(self.read_contact(self.header.ofsUser))
		
		offset = self.header.ofsFirstContact
		while offset <> 0:
			contact = self.read_contact(offset)
			offset = contact.ofsNext
			contactCount += 1
			self.verify_contact(contact)
		vassert(contactCount == self.header.contactCount, 'header.contactCount ('+str(self.header.contactCount)+') doesn\'t match actual count ('+str(contactCount)+')')
	
	def verify_contact(self, contact):
		self.totalUsed += contact.size()
		prefix = 'Contact #'+str(contact.contactID)+': '
		
		# Duplicate IDs
		vassert(not(contact.contactID in self.contactIDs), prefix+'Duplicate contact ID')
		self.contactIDs[contact.contactID] = True
		
		self.verify_settings(contact.ofsFirstSettings)
		
		self.expand_contact(contact)
		
		# All contacts have protocols
		vassert(contact.protocol <> None, prefix+'No protocol declared')
		
		allowed_ids = [contact.contactID]
		
		# Meta contact exists
		meta1_id = contact.get_meta_parent()
		if meta1_id <> None:
			meta1 = self.contact_by_id(meta1_id)
			vassert(meta1 <> None, prefix+'Bad metacontact ID: '+str(meta1_id))
			vassert(meta1.is_meta(), prefix+'Contact '+str(meta1_id)+' specified as meta but is not meta')
			# No multilevel meta
			vassert(not contact.is_meta(), prefix+'Links to parent meta while being meta itself')
			# Meta knows this child
			meta1_children = meta1.get_meta_children()
			vassert(contact.contactID in meta1_children, prefix+"Points to meta "+str(meta1_id)+' which doesn\'t have it as child')
		
		is_meta = contact.is_meta()
		if is_meta:
			meta_children = contact.get_meta_children()
			meta_count = contact.get_meta_child_count()
			vassert(len(meta_children) == meta_count, prefix+"Wrong number of meta children ("+str(meta_count)+' given, '+str(len(meta_children))+' listed)')
			allowed_ids += meta_children
			for childId in meta_children:
				child1 = self.contact_by_id(childId)
				vassert(child1 <> None, prefix+'Cannot find meta child '+str(childId))
				child1_parent = child1.get_meta_parent()
				vassert(child1_parent==contact.contactID, prefix+'Child '+str(childId)+' doesn\'t consider us parent (has '+str(child1_parent)+' instead)')

		(eventCount, ofsLastEvent) = self.verify_event_chain(contact.ofsFirstEvent, allowed_ids)
		
		vassert(contact.ofsLastEvent == ofsLastEvent, prefix+"ofsLastEvent doesn\'t match ("+str(contact.ofsLastEvent)+' given, '+str(ofsLastEvent)+' found)')
		# Allow actual eventCount to match EXACTLY 0 if this is a meta-child + parent has corrent number of our events
		if (meta1_id == None) or (eventCount <> 0):
			vassert(contact.eventCount == eventCount, prefix+"eventCount doesn\'t match ("+str(contact.eventCount)+' given, '+str(eventCount)+' actual)')
		else:
			eventCount = self.count_events(meta1.ofsFirstEvent, contact.contactID)
			vassert(contact.eventCount == eventCount, prefix+"eventCount doesn\'t match ("+str(contact.eventCount)+' given, '+str(eventCount)+' actual, stored in meta parent)')
	
	def verify_settings(self, offset):
		while offset <> 0:
			module = self.read(mirandadb.DBContactSettings(), offset)
			self.totalUsed += module.size()
			prefix = "Settings block "+str(offset)
			
			vassert(module.ofsModuleName in self.moduleOffsets, prefix+': ofsModuleName '+str(module.ofsModuleName)+' doesn\'t match any of the known modules')
			offset = module.ofsNext
	
	def verify_event_chain(self, offset, allowed_ids):
		eventCount = 0
		lastOffset = 0
		lastTimestamp = 0
		while offset <> 0:
			eventCount += 1
			event = self.read_event(offset)
			self.totalUsed += event.size()
			prefix = "Event "+str(offset)
			
			vassert(event.ofsPrev == lastOffset, prefix+': ofsPrev='+str(event.ofsPrev)+' doesn\'t match the previous event ('+str(lastOffset)+')')
			vassert(event.ofsModuleName in self.moduleOffsets, prefix+': ofsModuleName '+str(event.ofsModuleName)+' doesn\'t match any of the known modules')
			vassert(event.timestamp >= lastTimestamp, prefix+': timestamp='+str(event.timestamp)+' < last timestamp '+str(lastTimestamp))
			lastTimestamp = event.timestamp
			vassert(event.contactID in allowed_ids, prefix+': contactID='+str(event.contactID)+' is not in a list of allowed IDs (the contact and its meta children)')
			
			unkflags = event.flags & ~(event.DBEF_SENT | event.DBEF_READ | event.DBEF_RTL | event.DBEF_UTF | event.DBEF_ENCRYPTED)
			vassert(unkflags == 0, prefix+': Unknown flags: '+str(event.flags))
			
			lastOffset = offset
			offset = event.ofsNext
		return (eventCount, lastOffset)

	# Counts the number of events in the chain belonging to a particular ID.
	# Doesn't do anything else, doesn't increase totalUsed
	def count_events(self, offset, id):
		eventCount = 0
		while offset <> 0:
			event = self.read_event(offset)
			if event.contactID == id:
				eventCount += 1
			offset = event.ofsNext
		return eventCount


def verify_db(args):
	verifier = DbVerifier(args.dbname)
	verifier.verify()



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
	contacts_map = mirdiff.map_contacts(contacts1, contacts2)
	for (contact1, contact2) in contacts_map.items():
		if (contact1 == None) or (contact2 == None): continue
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
				print "Deleting: "+mirandadb.format_event(db2, e2)
				print "Matches: "+str(e1_match.offset)
				db2.delete_event(e2)
			else:
				print "Skipping: "+mirandadb.format_event(db2, e2)


# Main
parser = argparse.ArgumentParser(description="Analyzes Miranda database for corruption.",
	parents=[coreutils.argparser()])
parser.add_argument("dbname", help='path to database file')
parser.add_argument("--write", help='opens the databases for writing (WARNING: enables editing functions!)', action='store_true')
subparsers = parser.add_subparsers(title='subcommands')

sparser = subparsers.add_parser('verify', help='verifies database integrity')
sparser.set_defaults(func=verify_db)

sparser = subparsers.add_parser('dump-events', formatter_class=coreutils.SmartFormatter,
	help='prints all events for the given contacts',
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
