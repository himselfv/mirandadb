# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import struct
import io

# Miranda dbx_mmap database reader
log = logging.getLogger('miranda-dbx_mmap')

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

	def __str__(self):
		return str({
			'signature': self.signature,
			'ofsNext': self.ofsNext,
			'ofsFirstSettings': self.ofsFirstSettings,
			'eventCount': self.eventCount,
			'ofsFirstEvent': self.ofsFirstEvent,
			'ofsLastEvent': self.ofsLastEvent,
			'ofsFirstUnread': self.ofsFirstUnread,
			'tsFirstUnread': self.tsFirstUnread,
			'dwContactID': self.dwContactID
		})

	# Expands some data by seeking and reading it from file:
	#   Settings (and their module names)
	settings = None
	def expand(self, file):
		if self.settings == None:
			self.settings = self.parse_settings(file)
		return self.settings
	
	# Returns a list of {moduleName -> {settingName -> value}}
	def parse_settings(self, file):
		list = {}
		ofsSettings = self.ofsFirstSettings
		while ofsSettings > 0:
			settings = DBContactSettings()
			file.seek(ofsSettings, 0)
			settings.read(file)
			settings.expand(file)
			list[settings.moduleName] = settings
			ofsSettings = settings.ofsNext
		return list


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
	def unpack(self, tuple):
		(self.signature,
		self.ofsNext,
		self.ofsModuleName,
		self.cbBlob
		) = tuple
	
	def read(self, file):
		# read the static part
		super(DBContactSettings, self).read(file)
		# blob can be larger that needed so have to read everything ahead
		self.blob = file.read(self.cbBlob)
	
	moduleName = None
	def expand(self, file):
		if self.moduleName == None:
			file.seek(self.ofsModuleName, 0)
			dbname = DBModuleName()
			dbname.read(file)
			self.moduleName = dbname.name
		self.get_settings()
	
	_settings = None
	def get_settings(self):
		if self._settings == None:
			self._settings = self.parse_settings()
		return self._settings
	
	def parse_settings(self):
		# read until first cbName == 0
		if len(self.blob) <= 0:
			return list
		list = {}
		blobIo = io.BytesIO(self.blob)
		while True:
			setting = DBSetting()
			setting.read(blobIo)
			if setting.name == None:
				break
			list[setting.name] = setting
		return list

	def __str__(self):
		ret = self.moduleName + "\n"
		settings = self.get_settings()
		for setting in settings:
			ret += '  ' + str(settings[setting]) + "\n"
		return ret


"""
DBSetting:
	BYTE settingNameLen
	CHAR settingName[settingNameLen]
	DBVariant value

DBVariant:
BYTE type;
union {
	BYTE bVal; char cVal;
	WORD wVal; short sVal;
	DWORD dVal; long lVal;
	struct {
		union {
			char *pszVal;
			wchar_t *pwszVal;
		};
		WORD cchVal;   //only used for db/contact/getsettingstatic
	};
	struct {
		WORD cpbVal;
		BYTE *pbVal;
	};
};

For settings up to 128, their length is implicit and currently their DBVT_* code equals it.
For settings >= 128, they are:
  BYTE type
  WORD len
  CHAR data[len]
"""
class DBSetting(DBStruct):
	DBVT_DELETED	= 0		# this setting just got deleted, no other values are valid
	DBVT_BYTE		= 1		# bVal and cVal are valid
	DBVT_WORD		= 2		# wVal and sVal are valid
	DBVT_DWORD		= 4		# dVal and lVal are valid
	DBVT_ASCIIZ		= 255	# pszVal is valid
	DBVT_BLOB		= 254	# cpbVal and pbVal are valid
	DBVT_UTF8		= 253	# pszVal is valid
	DBVT_WCHAR		= 252	# pwszVal is valid

	DBVT_ENCRYPTED	= 250
	DBVT_UNENCRYPTED= 251
	
	DBVTF_VARIABLELENGTH = 0x80
	
	class Deleted:			# used in place of value for DBVT_DELETED entries
		pass
	
	name = None			# Setting name
	value = None		# Setting value, may be of different types
	type = None			# Setting type, for reference
	
	def read(self, file):
		namelen = struct.unpack("B", file.read(1))[0]
		# if name.len == 0, this is a stop sign in a setting chain
		if namelen <= 0:
			return
		(self.name,	self.type) = struct.unpack(str(namelen)+"sB", file.read(namelen+1))
		# read the dynamic part
		if self.type == self.DBVT_DELETED:
			self.value = Deleted()
		elif self.type == self.DBVT_BYTE:
			self.value = file.read(1)
		elif self.type == self.DBVT_WORD:
			self.value = struct.unpack('H', file.read(2))[0]
		elif self.type == self.DBVT_DWORD:
			self.value = struct.unpack('I', file.read(4))[0]
		elif self.type >= self.DBVTF_VARIABLELENGTH:
			datalen = struct.unpack('H', file.read(2))[0]
			data = file.read(datalen)
			if self.type == self.DBVT_ASCIIZ:
				self.value = data.decode('mbcs')
			elif self.type == self.DBVT_BLOB:
				self.value = data
			elif self.type == self.DBVT_UTF8:
				self.value = data.decode('utf-8')
			elif self.type == self.DBVT_WCHAR:
				self.value = data.decode('ucs-16')
			elif (self.type == self.DBVT_ENCRYPTED
			  or self.type == self.DBVT_UNENCRYPTED):
				self.value = data # cannot decrypt anything at this point
			else:
				raise Exception('Invalid data type in setting entry: '+self.type_to_str(self.type))
		else:
			raise Exception('Invalid data type in setting entry'+self.type_to_str(self.type))

	def type_to_str(self, type):
		if self.type == self.DBVT_DELETED:
			return "DBVT_DELETED"
		elif self.type == self.DBVT_BYTE:
			return "DBVT_BYTE"
		elif self.type == self.DBVT_WORD:
			return "DBVT_WORD"
		elif self.type == self.DBVT_DWORD:
			return "DBVT_DWORD"
		elif self.type == self.DBVT_ASCIIZ:
			return "DBVT_ASCIIZ"
		elif self.type == self.DBVT_BLOB:
			return "DBVT_BLOB"
		elif self.type == self.DBVT_UTF8:
			return "DBVT_UTF8"
		elif self.type == self.DBVT_WCHAR:
			return "DBVT_WCHAR"
		elif self.type == self.DBVT_ENCRYPTED:
			return "DBVT_ENCRYPTED"
		elif self.type == self.DBVT_UNENCRYPTED:
			return "DBVT_UNENCRYPTED"
		else:
			return str(type) # whatever is in there

	def type_str(self):
		return self.type_to_str(self.type)
	
	def __str__(self):
		return self.name+' ('+self.type_str()+') '+str(self.value)


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

flags:
#define DBEF_SENT       2  // this event was sent by the user. If not set this event was received.
#define DBEF_READ       4  // event has been read by the user. It does not need to be processed any more except for history.
#define DBEF_RTL        8  // event contains the right-to-left aligned text
#define DBEF_UTF       16  // event contains a text in utf-8
#define DBEF_ENCRYPTED 32  // event is encrypted (never reported outside a driver)
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
		self.user.expand(self.file)

	# Reads and unpacks data at a given offset or where the pointer is now
	# cl must provide cl.FORMAT and cl.unpack()
	def read(self, cl, offset = None):
		if offset <> None:
			self.file.seek(offset, 0)
		cl.read(self.file)
		log.debug(vars(cl))
		return cl
	
	# Returns a list of all DBContact()s
	_contacts = None
	def contacts(self):
		if self._contacts == None:
			self._contacts = []
			contactOffset = self.header.ofsFirstContact
			while contactOffset <> 0:
				contact = self.read(DBContact(), contactOffset)
				self._contacts.append(contact)
				contactOffset = contact.ofsNext
			for contact in self._contacts:
				contact.expand(self.file)
		return self._contacts


# Can be called manually for testing
def main():
	parser = argparse.ArgumentParser(description="Parse and print Miranda.")
	parser.add_argument("dbname", help='path to database file')
	parser.add_argument('--debug', action='store_const', const=logging.DEBUG, default=logging.WARNING,
		help='enable debug output')
	parser.add_argument("--dump-contacts", help='prints all contacts', action='store_true')
	parser.add_argument("--dump-modules", help='prints all modules', action='store_true')
	parser.add_argument("--dump-settings", help='prints all settings for the given contact', type=str, action='append')
	args = parser.parse_args()
	
	logging.basicConfig(level=args.debug, format='%(levelname)-8s %(message)s')
	
	db = MirandaDbxMmap(args.dbname)
	
	if args.dump_contacts:
		dump_contacts(db)
	
	if args.dump_modules:
		dump_modules(db)
	
	if args.dump_settings:
		for contact_name in args.dump_settings:
			dump_settings(db, contact_name)

def dump_contacts(db):
	totalEvents = 0
	for contact in db.contacts():
		print str(contact)
		totalEvents += contact.eventCount
	print "Total events: "+str(totalEvents)

def dump_modules(db):
	moduleOffset = db.header.ofsModuleNames
	totalModules = 0
	while moduleOffset <> 0:
		module = db.read(DBModuleName(), moduleOffset)
		print "Module: "+module.name
		totalModules += 1
		moduleOffset = module.ofsNext

def dump_settings(db, contact):
	print contact
	if len(contact) <= 0:
		contact = db.user
	else:
		contact = db.contacts()[contact]
	
	for name in contact.settings:
		print str(contact.settings[name])

if __name__ == "__main__":
	sys.exit(main())
