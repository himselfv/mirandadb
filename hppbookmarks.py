# -*- coding: utf-8 -*-
import sys
import argparse
import io
import coreutils
import mirandadb

log = mirandadb.log

#
# History++ Bookmarks for a contact
# See hpp_bookmarks.pas
#
class HPPBookmarksHeader(mirandadb.DBStruct):
	FORMAT = "=H"
	FIELDS = [
		'rec_size',		# One bookmark record size
						# Different versions apparently kept different-sized bookmarks
	]
class HPPBookmarkData(mirandadb.DBStruct):
	FORMAT = "=III"
	FIELDS = [
		'eventOffset',
		'CRC32',
		'timestamp'
	]
	SIZE = 3*4

# Reads and parses bookmark structures for one DBContact entry
def read_bookmarks(db, contact):
	blob = contact.get_setting("HistoryPlusPlus", "Bookmarks", [])
	if not blob:
		return []
	reader = io.BytesIO(blob)
	header = HPPBookmarksHeader()
	header.read(reader)
	if hasattr(header, 'problem'):
		log(header.problem)
		return []
	if header.rec_size <> HPPBookmarkData.SIZE:	# We support only modern sized bookmarks for now
		log("Unsupported bookmark entry size: "+str(header.rec_size)+' for contact '+contact.contactID)
	bookmarks = []
	for i in range((len(blob)-header.size())/header.rec_size):
		bookmark = HPPBookmarkData()
		bookmark.read(reader)
		if hasattr(header, 'problem'):
			log(header.problem)
		# We could've checked CRC but honestly, we don't care
		# Cached CRC and timestamp are mostly used in the plugin to sort bookmarks and verify their uniqueness without fetching all event contents
		bookmarks += [bookmark]
	return bookmarks

# Adds new bookmarks to bookmarks, skipping duplicates
def merge(bookmarks, new_bookmarks):
	# For now simply adds them together
	bookmarks += new_bookmarks
	return bookmarks

"""
MetaContacts creates confusion. Child events can be stored:
 - In child contacts
 - In metas (while keeping their contactID==child)
Bookmarks add to this confusion:
 - Bookmarks may be stored in child contacts (even if child events are in meta)
 - Bookmarks may be stored in metas (even if child events are in children)
 - Meta may have duplicate child events
 - Duplicate events may have different eventOffsets (either Meta or Child copies may be correct, or both incorrect)
 - Bookmarked CRCs could have changed on database imports.

Therefore:
1. For raw access, read DBContact() bookmarks directly with read_bookmarks().
2. For meta contacts, get_bookmarks() returns an amalgamation of meta bookmarks and all child bookmarks:
3. For child contacts, get_bookmarks() returns all bookmarks from the child, plus all bookmarks from its meta host only (no sibling contacts)
"""

# Retrieves all bookmark structures for the logical contact, minding MetaContacts
def get_bookmarks(db, contact):
	bookmarks = read_bookmarks(db, contact)
	if contact.is_meta():
		for child_id in contact.get_meta_children():
			child = db.contact_by_id(child_id)
			if not child: continue
			merge(bookmarks, read_bookmarks(db, child))
	return bookmarks

# Locates the bookmarked event or the best fit, if the event itself is unavailable.
# May return None if the bookmarked event cannot be located. This is regrettable but should be handled gracefully.
def find_event(db, contact, bookmark):
	try:
		event = db.read_event(bookmark.eventOffset)
		return event
	except mirandadb.SignatureError:
		pass
	print "Could not find event directly, looking up by timestamp"
	# Find the event by timestamp. HPP also selects by CRC but whatever
	return db.last_event_before_timestamp(contact, bookmark.timestamp+1)


# Can be called manually for testing
def main():
	parser = argparse.ArgumentParser(description="History++ bookmarks for a contact.",	parents=[coreutils.argparser()])
	parser.add_argument("dbname", help='path to database file')
	parser.add_argument('contact', type=str, nargs='*', help='print these contacts (default: all)')
	args = parser.parse_args()
	coreutils.init(args)
	
	db = mirandadb.MirandaDbxMmap(args.dbname)
	
	for contact in mirandadb.select_contacts_opt(db, args.contact):
		bookmarks = get_bookmarks(db, contact)
		if len(bookmarks) <= 0: continue
		print contact.display_name+' ('+str(contact.contactID)+'):'
		for bookmark in bookmarks:
			log.debug("Reading bookmark at "+str(bookmark.eventOffset))
			event = find_event(db, contact, bookmark)
			if not event:
				print '[Lost event with timestamp '+str(bookmark.timestamp)+']'
			else:
				print mirandadb.format_event(db, event)

if __name__ == "__main__":
	sys.exit(main())