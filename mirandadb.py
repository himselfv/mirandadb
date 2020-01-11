# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import struct

# Miranda dbx_mmap database reader

"""
See:
  plugins\Db3x_mmap_origin\src\database.h
  plugins\Db3x_mmap_origin\src\dbintf.h
  include\m_database.h
  include\m_db_int.h

DBHeader
|-->end of file (plain offset)
|-->first contact (DBContact)
|   |-->next contact (DBContact)
|   |   \--> ...
|   |-->first settings (DBContactSettings)
|   |	 |-->next settings (DBContactSettings)
|   |   |   \--> ...
|   |   \-->module name (DBModuleName)
|   \-->first/last/firstunread event
|-->user contact (DBContact)
|   |-->next contact = NULL
|   |-->first settings	as above
|   \-->first/last/firstunread event as above
\-->first module name (DBModuleName)
\-->next module name (DBModuleName)
\--> ...

"""

log = logging.getLogger('miranda-dbx_mmap')
logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s')


"""
BYTE signature[16];     // 'Miranda ICQ DB',0,26
						// in fact it's 'Miranda NG DBu',0,26
DWORD version;          // as 4 bytes, ie 1.2.3.10 = 0x0102030a
DWORD ofsFileEnd;       // offset of the end of the database - place to write new structures
DWORD slackSpace;       // a counter of the number of bytes that have been
									// wasted so far due to deleting structures and/or
									// re-making them at the end. We should compact when
									// this gets above a threshold
DWORD contactCount;     // number of contacts in the chain,excluding the user
DWORD ofsFirstContact;  // offset to first DBContact in the chain
DWORD ofsUser;          // offset to DBContact representing the user
DWORD ofsModuleNames;   // offset to first struct DBModuleName in the chain
"""
class DbHeader:
	FORMAT = '16sIIIIIII'
	def unpack(self, tuple):
		(self.signature,
		self.version,
		self.ofsFileEnd,
		self.slackSpace,
		self.contactCount,
		self.ofsFirstContact,
		self.ofsUser,
		self.ofsModuleNames
		) = tuple


"""
#define DBCONTACT_SIGNATURE   0x43DECADEu

DWORD signature;
DWORD ofsNext;          // offset to the next contact in the chain. zero if
// this is the 'user' contact or the last contact in the chain
DWORD ofsFirstSettings;	// offset to the first DBContactSettings in the chain for this contact.
DWORD eventCount;       // number of events in the chain for this contact
DWORD ofsFirstEvent,    // offsets to the first and
         ofsLastEvent;     // last DBEvent in the chain for this contact
DWORD ofsFirstUnread;   // offset to the first (chronological) unread event	in the chain, 0 if all are read
DWORD tsFirstUnread;    // timestamp of the event at ofsFirstUnread
DWORD dwContactID;
"""
class DbContact:
	FORMAT = "IIIIIIIII"
	def unpack(self, tuple):
		(self.signature,
		self.ofsNext,
		self.ofsFirstSettings,
		self.eventCount,
		self.ofsFirstEvent,
		self.ofsLastEvent,
		self.ofsFirstUnread,
		self.tsFirstUnread,
		self.dwContactID
		) = tuple
	

class MirandaDbxMmap:
	file = None
	
	def __init__(self, filename):
		self.file = open(filename, "rb")
		self.header = self.read(DbHeader())
		self.user = self.read(DbContact(), self.header.ofsUser)
		self.dump_contacts()

	# Reads and unpacks data at a given offset or where the pointer is now
	# cl must provide cl.FORMAT and cl.unpack()
	def read(self, cl, offset = None):
		if offset <> None:
			self.file.seek(offset, 0)
		# struct.* only reads from buffer so need to read bytes
		buffer = self.file.read(struct.calcsize(cl.FORMAT))
		tuple = struct.unpack(cl.FORMAT, buffer)
		cl.unpack(tuple)
		log.info(vars(cl))
		return cl
	
	def dump_contacts(self):
		contactOffset = self.header.ofsFirstContact
		totalEvents = 0
		while contactOffset <> 0:
			contact = self.read(DbContact(), contactOffset)
			totalEvents += contact.eventCount
			contactOffset = contact.ofsNext
		print totalEvents

# Can be called manually for testing
def main():
	parser = argparse.ArgumentParser(description="Parse and print Miranda.")
	parser.add_argument("dbname", help='path to database file')
	args = parser.parse_args()
	
	db = MirandaDbxMmap(args.dbname)

if __name__ == "__main__":
	sys.exit(main())
