# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import struct

# Miranda dbx_mmap database reader

log = logging.getLogger('miranda-dbx_mmap')
logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s')

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

# Inherit and override unpack() or read()
class DBStruct(object):
	def read(self, file):
		# struct.* only reads from buffer so need to read bytes
		buffer = file.read(struct.calcsize(self.FORMAT))
		tuple = struct.unpack(self.FORMAT, buffer)
		self.unpack(tuple)


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
class DBHeader(DBStruct):
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
class DBContact(DBStruct):
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

"""
#define DBMODULENAME_SIGNATURE  0x4DDECADEu
DWORD signature;
DWORD ofsNext;          // offset to the next module name in the chain
BYTE cbName;            // number of characters in this module name
char name[1];           // name, no nul terminator
"""
class DBModuleName(DBStruct):
	FORMAT = "IIB"
	def read(self, file):
		# read the static part
		super(DBModuleName, self).read(file)
		# read the dynamic part
		self.name = file.read(self.cbName).decode('ascii')
	def unpack(self, tuple):
		(self.signature,
		self.ofsNext,
		self.cbName
		) = tuple

"""
#define DBCONTACTSETTINGS_SIGNATURE  0x53DECADEu
DWORD signature;
DWORD ofsNext;          // offset to the next contactsettings in the chain
DWORD ofsModuleName;   // offset to the DBModuleName of the owner of these settings
DWORD cbBlob;           // size of the blob in bytes. May be larger than the
// actual size for reducing the number of moves
// required using granularity in resizing
BYTE blob[1];           // the blob. a back-to-back sequence of DBSetting
// structs, the last has cbName = 0
"""
class DBContactSettings(DBStruct):
	FORMAT = "IIII"
	def read(self, file):
		# read the static part
		super(DBContactSettings, self).read(file)
		# read the dynamic part
		self.blob = file.read(self.cbBlob)
	
	def unpack(self, tuple):
		(self.signature,
		self.ofsNext,
		self.ofsModuleName,
		self.cbBlob
		) = tuple

"""
#define DBEVENT_SIGNATURE  0x45DECADEu
DWORD signature;
MCONTACT contactID;     // a contact this event belongs to
DWORD ofsPrev, ofsNext;	// offset to the previous and next events in the
// chain. Chain is sorted chronologically
DWORD ofsModuleName;	   // offset to a DBModuleName struct of the name of
// the owner of this event
DWORD timestamp;        // seconds since 00:00:00 01/01/1970
DWORD flags;            // see m_database.h, db/event/add
WORD  wEventType;       // module-defined event type
DWORD cbBlob;           // number of bytes in the blob
BYTE  blob[1];          // the blob. module-defined formatting
"""
class DBEvent(DBStruct):
	FORMAT = "IIIIIIIHI"
	def read(self, file):
		# read the static part
		super(DBEvent, self).read(file)
		# read the dynamic part
		self.blob = file.read(self.cbBlob)
	
	def unpack(self, tuple):
		(self.signature,
		self.contactID,
		self.ofsPrev,
		self.ofsNext,
		self.ofsModuleName,
		self.timestamp,
		self.flags,
		self.wEventType,
		self.cbBlob
		) = tuple


class MirandaDbxMmap:
	file = None
	
	def __init__(self, filename):
		self.file = open(filename, "rb")
		self.header = self.read(DBHeader())
		self.user = self.read(DBContact(), self.header.ofsUser)

	# Reads and unpacks data at a given offset or where the pointer is now
	# cl must provide cl.FORMAT and cl.unpack()
	def read(self, cl, offset = None):
		if offset <> None:
			self.file.seek(offset, 0)
		cl.read(self.file)
		log.debug(vars(cl))
		return cl


# Can be called manually for testing
def main():
	parser = argparse.ArgumentParser(description="Parse and print Miranda.")
	parser.add_argument("dbname", help='path to database file')
	parser.add_argument("--dump-contacts", help='prints all contacts', action='store_true')
	parser.add_argument("--dump-modules", help='prints all modules', action='store_true')
	parser.add_argument("--dump-settings", help='prints all settings for the given contact', action='append', nargs=1)
	args = parser.parse_args()
	
	db = MirandaDbxMmap(args.dbname)
	
	if args.dump_contacts:
		dump_contacts(db)
	
	if args.dump_modules:
		dump_modules(db)
	
	if args.dump_settings:
		dump_settings(db, args.dump_settings)

def dump_contacts(db):
	contactOffset = db.header.ofsFirstContact
	totalEvents = 0
	while contactOffset <> 0:
		contact = db.read(DBContact(), contactOffset)
		totalEvents += contact.eventCount
		contactOffset = contact.ofsNext
	print totalEvents

def dump_modules(db):
	moduleOffset = db.header.ofsModuleNames
	totalModules = 0
	while moduleOffset <> 0:
		module = db.read(DBModuleName(), moduleOffset)
		print "Module: "+module.name
		totalModules += 1
		moduleOffset = module.ofsNext

def dump_settings(db):
	#TODO
	pass

if __name__ == "__main__":
	sys.exit(main())
