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
		'eventHandle',
		'CRC32',
		'timestamp'
	]
	SIZE = 3*4

# Retrieves all bookmark structures for the contact, minding MetaContacts
def hpp_get_bookmarks(db, contact):
	bookmarks = hpp_read_bookmarks(db, contact)
	if contact.is_meta():
		for child_id in contact.get_meta_children():
			child = db.contact_by_id(child_id)
			if not child: continue
			bookmarks += hpp_read_bookmarks(db, child)
	return bookmarks

# Reads and parses bookmark structures for one contact
def hpp_read_bookmarks(db, contact):
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

# Locates the bookmarked event or the best fit, if the event itself is unavailable.
# May return None if the bookmarked event cannot be located. This is regrettable but should be handled gracefully.
def hpp_find_event(db, contact, bookmark):
	try:
		event = db.read_event(bookmark.eventHandle)
		return event
	except mirandadb.SignatureError:
		pass
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
		bookmarks = hpp_get_bookmarks(db, contact)
		if len(bookmarks) <= 0: continue
		print contact.display_name+':'
		for bookmark in bookmarks:
			log.debug("Reading bookmark at "+str(bookmark.eventHandle))
			event = hpp_find_event(db, contact, bookmark)
			if not event:
				print '[Lost event with timestamp '+str(bookmark.timestamp)+']'
			else:
				print mirandadb.format_event(db, event)

if __name__ == "__main__":
	sys.exit(main())