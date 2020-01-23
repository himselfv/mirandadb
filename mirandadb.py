# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import struct
import io
import os, codecs, locale
import coreutils
import pprint # pretty printing
import fnmatch # wildcard matching
import utfutils


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

#
# Inherit and override unpack() or read()
# If self.FORMAT is present, its going to be automatically read and passed to unpack().
# If self.SIGNATURE is present, its going to be compared to self.signature.
#
class DBStruct(object):
	def read(self, file):
		self.offset = file.tell()	# store offset to help track the origin
		if hasattr(self, 'FORMAT'):
			# struct.* only reads from buffer so need to read bytes
			buffer = file.read(struct.calcsize(self.FORMAT))
			tuple = struct.unpack(self.FORMAT, buffer)
			self.unpack(tuple)
		if hasattr(self, 'SIGNATURE'):
			if self.signature <> self.SIGNATURE:
				raise Exception(type(self).__name__+': expected signature '+str(self.SIGNATURE)+', found '+str(self.signature))


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
	FORMAT = '=16sIIIIIII'
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
DWORD signature;
DWORD ofsNext;          // offset to the next module name in the chain
BYTE cbName;            // number of characters in this module name
char name[1];           // name, no nul terminator
"""
class DBModuleName(DBStruct):
	SIGNATURE = 0x4DDECADE
	FORMAT = "=IIB"
	def read(self, file):
		# read the static part
		super(DBModuleName, self).read(file)
		# read the dynamic part
		self.name = unicode(file.read(self.cbName).decode('ascii'))
	def unpack(self, tuple):
		(self.signature,
		self.ofsNext,
		self.cbName
		) = tuple


"""
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
	SIGNATURE = 0x43DECADE
	FORMAT = "=IIIIIIIII"
	def unpack(self, tuple):
		(self.signature,
		self.ofsNext,
		self.ofsFirstSettings,
		self.eventCount,
		self.ofsFirstEvent,
		self.ofsLastEvent,
		self.ofsFirstUnread,
		self.tsFirstUnread,
		self.contactID
		) = tuple

	def __str__(self):
		return unicode({
			'signature': self.signature,
			'ofsNext': self.ofsNext,
			'ofsFirstSettings': self.ofsFirstSettings,
			'eventCount': self.eventCount,
			'ofsFirstEvent': self.ofsFirstEvent,
			'ofsLastEvent': self.ofsLastEvent,
			'ofsFirstUnread': self.ofsFirstUnread,
			'tsFirstUnread': self.tsFirstUnread,
			'contactID': self.contactID
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
			list[settings.moduleName.lower()] = settings
			ofsSettings = settings.ofsNext
		return list

	# Retrieves setting value or None
	def get_setting(self, moduleName, settingName, default = None):
		moduleName = moduleName.lower()
		if not moduleName in self.settings:
			return default
		moduleSettings = self.settings[moduleName]
		settingName = settingName.lower()
		setting = moduleSettings[settingName]
		if setting == None:
			return default
		return setting.value

	# Access by index or name. Returns DBContactSettings (module settings).
	# Querying a pair will return you DBSetting.
	def __getitem__(self, arg):
		if isinstance(arg, tuple):
			return self[arg[0]][arg[1]]
		if isinstance(arg, (int, long)):
			return self._settings[arg]
		arg = arg.lower()
		for setting in self.settings():
			if setting.moduleName == arg:
				return setting.value
		return None


"""
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
	SIGNATURE = 0x53DECADE
	FORMAT = "=IIII"
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
		self.settings()
	
	_settings = None
	def settings(self):
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
			list[setting.name.lower()] = setting
		return list

	def __str__(self):
		ret = self.moduleName + "\n"
		settings = self.settings()
		for setting in settings:
			ret += '  ' + unicode(settings[setting]) + "\n"
		return ret

	# Access by index or name. Returns DBSetting object or None
	def __getitem__(self, arg):
		settings = self.settings()
		if isinstance(arg, (int, long)):
			return settings[arg]
		arg = arg.lower()
		for setting in settings:
			if setting == arg:
				return settings[setting]
		return None
    
	# Iteration
	def __iter__(self):
		return self.Iter(self, 0)
	class Iter:
		def __init__(self, module, start=0):
			self.module = module
			self.idx = start
		def __iter__(self):
			return self
		def next(self):
			if self.idx < len(self.module._settings):
				self.idx += 1
			else:
				raise StopIteration()
			return self.module._settings.values()[self.idx-1]


class Bytes(str):
	def __new__(cls, *args, **kw):
		return str.__new__(cls, *args, **kw)
	def __str__(self):
		return self.encode('hex')

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
		# Yes, the names are in UTF-8 too and there are live cases when this is used (e.g. ICQ server group names)
		self.name = self.name.decode('utf-8')
		# read the dynamic part
		if self.type == self.DBVT_DELETED:
			self.value = Deleted()
		elif self.type == self.DBVT_BYTE:
			self.value = struct.unpack('B', file.read(1))[0]
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
				self.value = Bytes(data)
			elif self.type == self.DBVT_UTF8:
				self.value = data.decode('utf-8')
			elif self.type == self.DBVT_WCHAR:
				self.value = data.decode('ucs-16')
			elif (self.type == self.DBVT_ENCRYPTED
			  or self.type == self.DBVT_UNENCRYPTED):
				self.value = Bytes(data) # cannot decrypt anything at this point
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
		return unicode(self.name)+u' ('+unicode(self.type_str())+u') '+unicode(self.value)


"""
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
	# Flags
	DBEF_SENT		= 2  # this event was sent by the user. If not set this event was received.
	DBEF_READ		= 4  # event has been read by the user. It does not need to be processed any more except for history.
	DBEF_RTL		= 8  # event contains the right-to-left aligned text
	DBEF_UTF		= 16 # event contains a text in utf-8
	DBEF_ENCRYPTED	= 32 # event is encrypted (never reported outside a driver)
	
	# Predefined common event types
	EVENTTYPE_MESSAGE		= 0
	EVENTTYPE_URL			= 1
	EVENTTYPE_CONTACTS		= 2				# data = DBContactsBlob [uin, nick]
	EVENTTYPE_ADDED			= 1000			# data = DBAuthBlob
	EVENTTYPE_AUTHREQUEST	= 1001			# data = DBAuthBlob
	EVENTTYPE_FILE			= 1002			# data = DBFileBlob [DWORD, filename, description]
	# Modules define their event types starting with this one:
	EVENTTYPE_MODULE_START	= 2000
	
	# The following events are de-facto standards, used by multiple modules
	
	# Widely used. Originally from NewXStatusNotify or TabSRMM.
	#   plugins\NewXstatusNotify\src\xstatus.h
	#   plugins\TabSRMM\src\msgs.h
	#   plugins\IEHistory\src\stdafx.h
	EVENTTYPE_STATUSCHANGE	= 25368
	
	# Widely used. Defined in two different ways (AVATAR_CHANGE and AVATARCHANGE)
	#   ExternalAPI\m_avatarhistory.h
	EVENTTYPE_AVATAR_CHANGE	= 9003
	
	# Origin unknown, usage unknown
	#   plugins\TabSRMM\src\msgs.h
	EVENTTYPE_ERRMSG		= 25366
	
	# The rest is module-specific; thankfully, most modules use only the standard ones

	# Jabber protocol
	#   protocols\JabberG\src\stdafx.h
	#   plugins\Scriver\src\msgs.h
	#   plugins\IEView\src\HTMLBuilder.h
	EVENTTYPE_JABBER_CHATSTATES		= 2000
	EVENTTYPE_JABBER_PRESENCE		= 2001

	# ICQ protocol
	#   include\m_icq.h
	#   plugins\NewEventNotify\src\stdafx.h		ICQEVENTTYPE_SMSCONFIRMATION
	#   plugins\SMS\src\SMSConstans.h			ICQEVENTTYPE_SMSCONFIRMATION
	# And in a lot of other places
	ICQEVENTTYPE_SMS				= 2001
	ICQEVENTTYPE_EMAILEXPRESS		= 2002
	ICQEVENTTYPE_WEBPAGER			= 2003
	ICQEVENTTYPE_MISSEDMESSAGE		= 2004
	ICQEVENTTYPE_SMSCONFIRMATION	= 3001

	# WaTrack
	#   plugins\ExternalAPI\m_music.h
	#   plugins\HistoryStats\src\statistic.h
	EVENTTYPE_WAT_REQUEST			= 9601
	EVENTTYPE_WAT_ANSWER			= 9602
	EVENTTYPE_WAT_ERROR				= 9603
	EVENTTYPE_WAT_MESSAGE			= 9604

	# Steam
	#   protocols\Steam\src\stdafx.h
	EVENTTYPE_STEAM_CHATSTATES		= 2000

	# Facebook
	#   protocols\FacebookRM\src\constants.h
	FACEBOOK_EVENTTYPE_CALL			= 10010
	
	# VK
	#   protocols\VKontakte\src\vk.h
	VK_USER_DEACTIVATE_ACTION		= 9321

	# SkypeWeb
	#  protocols\SkypeWeb\src\skype_db.h
	SKYPE_DB_EVENT_TYPE_ACTION				= 10001
	SKYPE_DB_EVENT_TYPE_INCOMING_CALL		= 10002
	SKYPE_DB_EVENT_TYPE_CALL_INFO			= 10003
	SKYPE_DB_EVENT_TYPE_FILETRANSFER_INFO	= 10004
	SKYPE_DB_EVENT_TYPE_URIOBJ				= 10005
	SKYPE_DB_EVENT_TYPE_EDITED_MESSAGE		= 10006
	SKYPE_DB_EVENT_TYPE_MOJI				= 10007
	SKYPE_DB_EVENT_TYPE_FILE				= 10008
	SKYPE_DB_EVENT_TYPE_UNKNOWN				= 10009

	# Twitter
	#   protocols\Twitter\src\stdafx.h
	TWITTER_DB_EVENT_TYPE_TWEET				= 2718
	
	# Tox
	#   protocols\Tox\src\stdafx.h
	# A message describing an user action. This is similar to /me (CTCP ACTION) on IRC.
	TOX_DB_EVENT_ACTION						= 10000 + 1
	# Probably an edit, but nothing defines TOX_MESSAGE_TYPE_CORRECTION and nothing uses this
	#TOX_DB_EVENT_CORRECTION				= 10000 + TOX_MESSAGE_TYPE_CORRECTION


	SIGNATURE = 0x45DECADE
	FORMAT = "=IIIIIIIHI"
	def unpack(self, tuple):
		(self.signature,
		self.contactID,
		self.ofsPrev,
		self.ofsNext,
		self.ofsModuleName,
		self.timestamp,
		self.flags,
		self.eventType,
		self.cbBlob
		) = tuple
	
	def read(self, file):
		# read the static part
		super(DBEvent, self).read(file)
		# read the dynamic part
		self.blob = file.read(self.cbBlob)


#
# Event content types
#
class DBEventBlob(DBStruct):
	def __init__(self, unicode, file = None):
		self.unicode = unicode
		# "file" may be a Bytes() instead
		if file <> None:
			read_op = getattr(file, "read", None)
			if not callable(read_op):
				file = io.BytesIO(file)
			self.read(file)
	
	def read_str(self, file):
		s = ""
		while True:
			c = file.read(1)
			if len(c) == 0:
				log.warning('No more bytes where string is expected in event data (read:'+s.encode('hex')+')')
				self.has_problems = True
				break
			if c == chr(0):
				break
			s = s + c
		if self.unicode:
			return s.decode('utf-8')
		else:
			return unicode(s.decode('mbcs'))

class DBAuthBlob(DBEventBlob):
	#[uin:DWORD, hContact:DWORD, nick, firstName, lastName, email, reason]
	FORMAT = "=II"
	def unpack(self, tuple):
		(self.uin,
		self.hContact
		) = tuple
	
	def read(self, file):
		super(DBEventBlob, self).read(file)
		self.nick = self.read_str(file)
		self.firstName = self.read_str(file)
		self.lastName = self.read_str(file)
		self.email = self.read_str(file)
		self.reason = self.read_str(file)



class MirandaDbxMmap(object):
	file = None
	def __init__(self, filename):
		self.file = open(filename, "rb")
		self.filename = filename
		self.header = self.read(DBHeader())
		self.user = self.read(DBContact(), self.header.ofsUser)
		self.expand_contact(self.user)

	# Reads and unpacks data at a given offset or where the pointer is now
	# cl must provide cl.FORMAT and cl.unpack()
	def read(self, cl, offset = None):
		if offset <> None:
			#print "Seeking "+str(offset)
			self.file.seek(offset, 0)
		cl.read(self.file)
		log.debug(vars(cl))
		return cl

	#
	# Modules and protocols
	#
	_moduleNames = {}
	def get_module_name(self, ofsModule):
		if not ofsModule in self._moduleNames:
			module = self.read(DBModuleName(), ofsModule)
			self._moduleNames[ofsModule] = module.name
		return self._moduleNames[ofsModule]

	# For modules that are accounts, returns their base protocol (string)
	_baseProtocols = {}
	def get_base_proto(self, moduleName):
		if isinstance(moduleName, int):	# this is offset
			moduleName = self.get_module_name(moduleName)
			if moduleName == None: return None
		if not moduleName in self._baseProtocols:
			self._baseProtocols[moduleName] = self.user.get_setting(moduleName, "AM_BaseProto")
		return self._baseProtocols[moduleName]


	#
	# Contacts
	#
	
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
				self.expand_contact(contact)
		return self._contacts
	
	def expand_contact(self, contact):
		contact.expand(self.file)
		contact.protocol = contact.get_setting('Protocol', 'p')
		if contact.protocol <> None:
			contact.nick = contact.get_setting(contact.protocol, 'Nick')
		else:
			contact.nick = None
		# Guess some telling display name
		contact.display_name = contact.get_setting('CList', 'MyHandle')
		if contact.display_name == None:
			contact.display_name = contact.nick
		if contact.display_name == None:
			contact.display_name = u'#'+unicode(contact.contactID);
		if contact.protocol <> None:
			contact.display_name = contact.protocol + u'\\' + contact.display_name
		# Guess stable ID for some common protocols
		if contact.protocol <> None:
			contact.id = contact.get_setting(contact.protocol, "jid")		# xmpp
			if contact.id == None:
				contact.id = contact.get_setting(contact.protocol, "uin")	# ICQ
			if contact.id == None:
				contact.id = contact.get_setting(contact.protocol, "id")	# vkontakte
		else:
			contact.id = None
	
	# Returns a contact by its database contactID
	def contact_by_id(self, id):
		if id == 0:
			return self.user
		for contact in self.contacts():
			if contact.contactID == id:
				return contact
		return None
	
	# Returns all contacts with nickname matching the given one, or db.user if contact_name is empty
	def contacts_by_mask(self, contact_mask):
		if len(contact_mask) <= 0:
			return [self.user]
		if contact_mask == '*':
			return [self.user] + self.contacts()
		contact_mask=contact_mask.lower()
		log.warning("looking for: "+contact_mask);
		ret = []
		for contact in self.contacts():
			if contact.nick and fnmatch.fnmatch(contact.nick.lower(), contact_mask):
				ret.append(contact)
				continue
			if contact.display_name and fnmatch.fnmatch(contact.display_name.lower(), contact_mask):
				ret.append(contact)
				continue
			if contact.id and fnmatch.fnmatch(str(contact.id).lower(), contact_mask):
				ret.append(contact)
				continue
			if contact.id and fnmatch.fnmatch((contact.protocol+u'\\'+str(contact.id)).lower(), contact_mask):
				ret.append(contact)
				continue
			if '#'+str(contact.contactID) == contact_mask:
				ret.append(contact)
				continue
		log.warning('entries: '+str(len(ret)))
		return ret


	#
	# Events
	#
	def read_event(self, offset):
		return self.read(DBEvent(), offset)
	
	# Retrieves and decodes all events for the contact. Handles MetaContacts transparently.
	#	contact_id: Return only events for this contactId (MetaContacts can host multiple)
	#	with_metacontacts: Locate this contact events in MetaContacts too.
	#		This may be slower than printing out the entire MetaContact once,
	#		because EVERY subcontact will iterate ALL metacontact events, filtering them.
	def get_events(self, contact, with_metacontacts=True, contactId=None):
		# MetaContacts can steal events from their children but leave contactId and moduleName intact
		if (contact.ofsFirstEvent == 0) and (contact.eventCount > 0) and with_metacontacts:
			metaId = contact.get_setting("MetaContacts", "ParentMeta")
			metaContact = self.contact_by_id(metaId) if metaId <> None else None
			if metaContact <> None:
				contactId2 = contactId if contactId <> None else contact.contactID
				return self.EventIter(self, metaContact.ofsFirstEvent, contactId=contactId2)
		# If this is a MetaContact itself, skip events not directly owned by it
		if with_metacontacts and (contact.protocol == "MetaContacts") and (contactId == None):
			contactId = contact.contactID
		return self.EventIter(self, contact.ofsFirstEvent, contactId)
	class EventIter:
		def __init__(self, db, ofsFirstEvent, contactId=None):
			self.db = db
			self.offset = ofsFirstEvent
			self.contactId = contactId
		def __iter__(self):
			return self
		def next(self):
			while True and self.offset <> 0:
				event = self.db.read_event(self.offset)
				self.offset = event.ofsNext if event <> None else 0
				if (event == None) or (self.contactId == None) or (event.contactID == self.contactId):
					break
			if self.offset == 0: raise StopIteration()
			event.data = self.db.decode_event_data(event)
			return event
	
	# Returns either a string or something that can be vars()ed
	def decode_event_data(self, event):
		_unicode = (event.DBEF_UTF & event.flags) <> 0
		if event.flags & event.DBEF_ENCRYPTED: # Can't decrypt, return hex
			return {
				'type' : 'encrypted',
				'hex' : event.blob.encode('hex'),
				'unicode' : _unicode
				}
		elif event.eventType==event.EVENTTYPE_ADDED:
			blob = DBAuthBlob(_unicode, event.blob)
			if hasattr(blob, 'has_problems'):
				log.warning('Event@'+str(event.offset)+': DBAuthBlob has problems')
			return blob
		elif event.eventType==event.EVENTTYPE_AUTHREQUEST:
			blob = DBAuthBlob(_unicode, event.blob)
			if hasattr(blob, 'has_problems'):
				log.warning('Event@'+str(event.offset)+': DBAuthBlob has problems')
			return blob
		elif event.eventType==event.EVENTTYPE_MESSAGE:
			return self.decode_event_data_string(event, _unicode)
		else:
			return {
				'type' : 'unsupported',
				'hex' : event.blob.encode('hex'),
				'unicode' : _unicode
				}
	
	# Decodes event data as simple string
	def decode_event_data_string(self, event, _unicode):
		# Most event blobs are strings in one of the THREE formats:
		#  1. UTF-8 only 			modern/old when posted with PREF_UTF
		#  2. ANSI + UTF-16LE		old db, when posted with PREF_UNICODE
		#  3. ANSI only				old db, when posted with no PREF_*
		# With any format, there may be:
		#  - additional data, as defined by protocol
		#  - extra space, if preallocated
		# Note:
		#  * ANSI locale may be MULTIBYTE (e.g. shift-jis)
		#  * ANSI locale may CHANGE BETWEEN MESSAGES (if you changed your PC locale in the past)
		#  * For all of ANSI, UTF-8 and UTF-16, we must trim terminating NULLs, or they'll be decode()d as characters.
		if _unicode:
			(blob, tail) = utfutils.eatutf8(event.blob)
			ret = self.utf8trydecode(blob)
		else:
			(blob, tail) = utfutils.eatansi(event.blob)
			ret = self.mbcstrydecode(blob)		# The ANSI version
			# The tail may contain "UTF-16 version", "additional data" and "junk" and there's no flag to tell which is which
			# But the common code that added UTF-16 always added EXACTLY twice the ANSI bytes (even if multibyte ANSI required less),
			# so that's a pretty good indicator
			# We may also have MORE than that, and we may have accidental exact match for short messages,
			# but we'll ignore both possibilities for now (too rare in practice to study them)
			if (len(tail) > 0) and (len(tail) == 2*len(blob)+2): # +2b non-removed null
	 			# Actual UTF16 string may be shorter due to UTF-16 over-allocation (if ANSI had been MBCS and required less than twice the size)
	 			(utf16blob, tail) = utfutils.eatutf16(tail)
	 			utf16text = self.utf16trydecode(utf16blob)
	 			
	 			if 'problem' in utf16text:
	 				ret['utf16_problem'] = utf16text['problem']
	 				if not ('problem' in ret):
	 					ret['problem'] = 'Problem with UTF16 text'
	 				ret['utf16'] = utf16blob.encode('hex')
	 			# We can't verify UTF16==ANSI because:
	 			#  * ANSI may not be in our current locale
	 			#  * ANSI can't encode everything Unicode can
	 			# But we can check that at least it's not an obvious failure
	 			elif (len(utf16text['text']) < len(ret['text']) / 2):
	 				ret['problem'] = 'UTF16 tail doesn''t match at all'
	 				ret['utf16'] = utf16text['text']
	 			else:
	 				# Otherwise replace
	 				utf16text['ansi'] = ret['text']
	 				ret = utf16text
	 	proto = self.get_base_proto(event.ofsModuleName)
	 	if (len(tail) > 0) and (proto=="VKontakte"):
	 		# Modern versions of VKontakte store message IDs as ASCII text
	 		if len(tail) < 10:	# weed out obvious fails
	 			ret['vk-mid'] = tail
	 			tail = ''
		if len(tail) > 0:
			ret['remainder'] = tail.encode('hex')
			if not ('problem' in ret):
				ret['problem'] = "Remainder data in event"
		ret['unicode'] = _unicode
		if 'problem' in ret:
			log.warning('Event@'+str(event.offset)+': '+ret['problem'])
		return ret

	# Decodes MBCS text and verifies that it's not junk
	def mbcstrydecode(self, data):
		ret = {}
		try:
			ret['text'] = data.decode('mbcs')
			ret['mbcs'] = data.encode('hex') # TODO: remove
		except DecodeError:
			ret['problem'] = "Cannot decode as mbcs"
			ret['mbcs'] = data.encode('hex')
			return ret
		return ret

	# Decodes UTF8 text and verifies that it's not junk
	# Returns (True, the decoded text) or (False, hex text, problem description)
	def utf8trydecode(self, data):
		ret = {}
		try:
			text = data.decode('utf-8')
			ret['text'] = text
		except UnicodeDecodeError:
			ret['problem'] = "Cannot decode as utf-8"
			ret['utf8'] = data.encode('hex')
		return ret
	
	def utf16trydecode(self, data):
		ret = {}
		try:
			text = data.decode('UTF-16LE')	# LE, so that it doesn't eat bom, if it's present
			ret['text'] = text
			ret['utf16'] = data.encode('hex') # TODO: remove
		except UnicodeDecodeError as e:
			ret['problem'] = "Cannot decode as utf-16: "+str(e)
			ret['utf16'] = data.encode('hex')
		return ret


# Can be called manually for testing
def main():
	parser = argparse.ArgumentParser(description="Parse and print Miranda.",
		parents=[coreutils.argparser()])
	parser.add_argument("dbname", help='path to database file')
	parser.add_argument("--dump-modules", help='prints all module names', action='store_true')
	parser.add_argument("--dump-contacts-low", help='prints all contacts (low-level)', action='store_true')
	parser.add_argument("--dump-contacts", help='prints all contacts', action='store_true')
	parser.add_argument("--dump-settings", help='prints all settings for the given contact', type=str, action='append')
	parser.add_argument("--event-stats", help='collects event statistics', action='store_true')
	parser.add_argument("--dump-events", help='prints all events for the given contact', type=str, action='append')
	parser.add_argument("--bad-events", help='dumps only bad events', action='store_true')
	parser.add_argument("--unsupported-events", help='dumps only unsupported events', action='store_true')
	args = parser.parse_args()
	coreutils.init(args)
	
	db = MirandaDbxMmap(args.dbname)
	
	if args.dump_contacts_low:
		dump_contacts_low(db)
	
	if args.dump_contacts:
		dump_contacts(db)
	
	if args.dump_modules:
		dump_modules(db)
	
	if args.dump_settings:
		for contact_name in args.dump_settings:
			for contact in db.contacts_by_mask(contact_name):
				dump_settings(db, contact)
	
	if args.dump_events:
		params = {}
		params['bad_only'] = args.bad_events
		params['unsupported_only'] = args.unsupported_events
		for contact_name in args.dump_events:
			for contact in db.contacts_by_mask(contact_name):
				dump_events(db, contact, params)
	
	if args.event_stats:
		event_stats(db)

def dump_contacts_low(db):
	totalEvents = 0
	for contact in ([db.user] + db.contacts()):
		pprint.pprint(vars(contact))
		totalEvents += contact.eventCount

def dump_contacts(db):
	totalEvents = 0
	for contact in ([db.user] + db.contacts()):
		print unicode(contact.display_name)
		print u"  Protocol: "+unicode(contact.protocol)
		print u"  ID: "+unicode(contact.id)
		print u"  Contact ID: #"+unicode(contact.contactID)
		print u"  Nick: "+unicode(contact.nick)
		print u"  MyHandle: "+unicode(contact.get_setting('CList', 'MyHandle'))
		print u"  Group: "+unicode(contact.get_setting('CList', 'Group'))
		print u"  Hidden: "+unicode(contact.get_setting('CList', 'Hidden'))
		print u"  Events: "+unicode(contact.eventCount)
		totalEvents += contact.eventCount
	print "Total events: "+unicode(totalEvents)

def dump_modules(db):
	moduleOffset = db.header.ofsModuleNames
	totalModules = 0
	while moduleOffset <> 0:
		module = db.read(DBModuleName(), moduleOffset)
		print "Module: "+module.name
		totalModules += 1
		moduleOffset = module.ofsNext

def dump_settings(db, contact):
	display_name = ''
	if hasattr(contact, 'display_name') and contact.display_name:
		display_name = unicode(contact.display_name)
	if hasattr(contact, 'protocol') and contact.protocol:
		display_name += ' ('+contact.protocol+')'
	print display_name
	for name in contact.settings:
		print unicode(contact.settings[name])

def event_stats(db):
	stats = {}
	stats['count'] = 0
	stats['flags'] = {'sent': 0, 'read': 0, 'rtl': 0, 'utf': 0, 'encrypted': 0, 'other': 0}
	stats['unknown_flags'] = 0  # collects unknown bit flags
	stats['modules'] = {}
	stats['types'] = {}
	stats['blobSizes'] = {}
	event_stats_contact(db, db.user, stats)
	for contact in db.contacts():
		event_stats_contact(db, contact, stats)
	del stats['blobSizes'] # no point printing, too many messages of any size
	pprint.pprint(stats)

def event_stats_contact(db, contact, stats):
	ofsEvent = contact.ofsFirstEvent
	while ofsEvent <> 0:
		event = db.read(DBEvent(), ofsEvent)
		stats['count'] += 1
		
		moduleName = db.get_module_name(event.ofsModuleName)
		s_modules = stats['modules']
		s_modules[moduleName] = s_modules.get(moduleName, 0) + 1
		
		s_flags = stats['flags']
		if event.flags & event.DBEF_SENT:			s_flags['sent'] += 1
		elif event.flags & event.DBEF_READ:			s_flags['read'] += 1
		elif event.flags & event.DBEF_RTL:			s_flags['rtl'] += 1
		elif event.flags & event.DBEF_UTF:			s_flags['utf'] += 1
		elif event.flags & event.DBEF_ENCRYPTED:	s_flags['encrypted'] += 1
		other_flags = event.flags & ~(event.DBEF_SENT | event.DBEF_READ | event.DBEF_RTL | event.DBEF_UTF | event.DBEF_ENCRYPTED)
		if other_flags <> 0:
			s_flags['other'] += 1
			stats['unknown_flags'] = stats['unknown_flags'] | other_flags
		
		if event.eventType >= event.EVENTTYPE_MODULE_START:
			eventKey = (moduleName, event.eventType)
		else:
			eventKey = event.eventType
		s_types = stats['types']
		s_types[eventKey] = s_types.get(eventKey, 0) + 1
		
		s_blobSizes = stats['blobSizes']
		s_blobSizes[event.cbBlob] = s_blobSizes.get(event.cbBlob, 0) + 1
		
		ofsEvent = event.ofsNext

# Produces a pretty line describing the event
def format_event(db, event, data = None):
	if data == None:
		if hasattr(event, 'data') and (event.data <> None):	# Some events have pre-decoded data
			data = event.data
		else:
			data = db.decode_event_data(event)
	# Stringify data
	if isinstance(data, basestring):
		pass
	elif isinstance(data, dict):
		data = ', '.join([ repr(key) + ': ' + repr(value) for (key, value) in data.items()])
	else:
		data = unicode(vars(data))
	return str(event.timestamp) + " " + db.get_module_name(event.ofsModuleName) + " " + str(event.eventType) + " " + str(event.flags) + " " + data

def dump_events(db, contact, params):
	def should_print_event(event):
		if params['bad_only'] and isinstance(data, dict) and ('problem' in data):
			return True
		if params['unsupported_only'] and isinstance(data, dict) and (data.get('type', None) in ['unsupported', 'encrypted']):
			return True
		return not (params['bad_only'] or params['unsupported_only'])
	print "Events for "+contact.display_name+": "
	for event in db.get_events(contact):
		data = event.data
		if isinstance(data, dict) and ('problem' in data):
			data['offset'] = event.offset
		if not should_print_event(event):
			continue
		print format_event(db, event, data)

if __name__ == "__main__":
	sys.exit(main())
