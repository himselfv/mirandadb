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
from datetime import datetime # for datetime.now
import calendar


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
# If self.FIELDS is present, .pack()/.unpack() will be auto-generated
#
class DBStruct(object):
	def read(self, file):
		self.offset = file.tell()	# store offset to help track the origin
		if hasattr(self, 'FORMAT'):
			# struct.* only reads from buffer so need to read bytes
			buffer = file.read(struct.calcsize(self.FORMAT))
			_tuple = struct.unpack(self.FORMAT, buffer)
			if hasattr(self, 'FIELDS'):
				assert(len(_tuple)==len(self.FIELDS))
				for i in range(0, len(self.FIELDS)):
					setattr(self, self.FIELDS[i], _tuple[i])
			else:
				self.unpack(_tuple)
		if hasattr(self, 'SIGNATURE'):
			if self.signature <> self.SIGNATURE:
				raise Exception(type(self).__name__+': expected signature '+str(self.SIGNATURE)+', found '+str(self.signature))
	def write(self, file, offset=None):
		if offset <> None:
			file.seek(offset, 0)
		if hasattr(self, 'SIGNATURE'):
			self.signature = self.SIGNATURE
		if hasattr(self, 'FORMAT'):
			if hasattr(self, 'FIELDS'):
				_tuple = tuple(getattr(self, field) for field in self.FIELDS)
			else:
				_tuple = self.pack()
			buffer = struct.pack(self.FORMAT, *_tuple)
			file.write(buffer)
	def size(self):
		if hasattr(self, 'FORMAT'):
			return struct.calcsize(self.FORMAT)
		return 0

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
	FIELDS = [
		'signature',
		'version',
		'ofsFileEnd',
		'slackSpace',
		'contactCount',
		'ofsFirstContact',
		'ofsUser',
		'ofsModuleNames'
	]


"""
DWORD signature;
DWORD ofsNext;          // offset to the next module name in the chain
BYTE cbName;            // number of characters in this module name
char name[1];           // name, no nul terminator
"""
class DBModuleName(DBStruct):
	SIGNATURE = 0x4DDECADE
	FORMAT = "=IIB"
	FIELDS = [
		'signature',
		'ofsNext',
		'cbName'
	]
	def read(self, file):
		super(DBModuleName, self).read(file)
		self.name = unicode(file.read(self.cbName).decode('ascii'))
	def write(self, file, offset=None):
		nameBytes = self.name.encode('ascii')
		self.cbName = len(nameBytes)
		super(DBModuleName, self).write(file, offset)
		file.write(nameBytes)
	def size(self):
		return super(DBModuleName, self).size() + len(self.name.encode('ascii'))


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
	FIELDS = [
		'signature',
		'ofsNext',
		'ofsFirstSettings',
		'eventCount',
		'ofsFirstEvent',
		'ofsLastEvent',
		'ofsFirstUnread',
		'tsFirstUnread',
		'contactID'
	]
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

	# MetaContacts
	def get_meta_parent(self):
		return self.get_setting("MetaContacts", "ParentMeta")
	
	def is_meta(self):
		return self.protocol=='MetaContacts'
	
	def get_meta_child_count(self):
		return self.get_setting("MetaContacts", "NumContacts")
	
	def get_meta_children(self):
		ret = []
		module = self.settings["metacontacts"]
		if module == None: return ret
		i = 0
		while True:
			childId = module.get_setting("Handle"+str(i))
			if childId == None: break
			ret.append(childId)
			i += 1
		return ret


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
	FIELDS = [
		'signature',
		'ofsNext',
		'ofsModuleName',
		'cbBlob'
	]
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
	
	# settingName -> value
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

	def get_setting(self, settingName, default = None):
		setting = self[settingName]
		if setting == None:
			return default
		return setting.value


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
		if self.type == self.DBVT_DELETED:			return "DBVT_DELETED"
		elif self.type == self.DBVT_BYTE:			return "DBVT_BYTE"
		elif self.type == self.DBVT_WORD:			return "DBVT_WORD"
		elif self.type == self.DBVT_DWORD:			return "DBVT_DWORD"
		elif self.type == self.DBVT_ASCIIZ:			return "DBVT_ASCIIZ"
		elif self.type == self.DBVT_BLOB:			return "DBVT_BLOB"
		elif self.type == self.DBVT_UTF8:			return "DBVT_UTF8"
		elif self.type == self.DBVT_WCHAR:			return "DBVT_WCHAR"
		elif self.type == self.DBVT_ENCRYPTED:		return "DBVT_ENCRYPTED"
		elif self.type == self.DBVT_UNENCRYPTED:	return "DBVT_UNENCRYPTED"
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
	EVENTTYPE_MESSAGE		= 0				# MessageBlob
	EVENTTYPE_URL			= 1				# DBURLBlob
	EVENTTYPE_CONTACTS		= 2				# DBContactsBlob [uin, nick]
	EVENTTYPE_ADDED			= 1000			# DBAuthBlob
	EVENTTYPE_AUTHREQUEST	= 1001			# DBAuthBlob
	EVENTTYPE_FILE			= 1002			# DBFileBlob [DWORD, filename, description]
	# Modules define their event types starting with this one:
	EVENTTYPE_MODULE_START	= 2000
	
	# The following events are de-facto standards, used by multiple modules
	
	# Widely used. Originally from NewXStatusNotify or TabSRMM.
	#   plugins\NewXstatusNotify\src\xstatus.h
	#   plugins\TabSRMM\src\msgs.h
	#   plugins\IEHistory\src\stdafx.h
	EVENTTYPE_STATUSCHANGE	= 25368				# MessageBlob
	
	# Well known in two different ways (AVATAR_CHANGE and AVATARCHANGE).
	# Written by AvatarHistory but under contact's proto:
	#   ExternalAPI\m_avatarhistory.h
	EVENTTYPE_AVATAR_CHANGE	= 9003				# DBAvatarChangeBlob
	
	# Produced by plugins\TabSRMM\src\sendqueue.cpp\logError()
	# Very weird, tries to store its message in "module name" and (optionally) non-sent message in data,
	# so can contain basically anything.
	#   plugins\TabSRMM\src\msgs.h
	EVENTTYPE_ERRMSG		= 25366
	
	# The rest is module-specific; thankfully, most modules use only the standard ones

	# Jabber protocol
	#   protocols\JabberG\src\stdafx.h
	#   plugins\Scriver\src\msgs.h
	#   plugins\IEView\src\HTMLBuilder.h
	EVENTTYPE_JABBER_CHATSTATES		= 2000		# DBJabberChatStatesBlob
	EVENTTYPE_JABBER_PRESENCE		= 2001		# DBJabberPresenceBlob

	# ICQ protocol
	#   include\m_icq.h
	#   plugins\NewEventNotify\src\stdafx.h
	#   plugins\SMS\src\SMSConstans.h
	# And in a lot of other places
	ICQEVENTTYPE_SMS				= 2001		# DBICQSMSBlob
	ICQEVENTTYPE_EMAILEXPRESS		= 2002		# DBICQEmailExpressBlob
	ICQEVENTTYPE_WEBPAGER			= 2003		# DBICQWebPagerBlob
	ICQEVENTTYPE_MISSEDMESSAGE		= 2004		# DBICQMissedMessageBlob
	ICQEVENTTYPE_SMSCONFIRMATION	= 3001		# DBICQSMSConfirmationBlob

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
	VK_USER_DEACTIVATE_ACTION		= 9321		# DBVKontakteUserDeactivateActionBlob

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
	FIELDS = [
		'signature',
		'contactID',
		'ofsPrev',
		'ofsNext',
		'ofsModuleName',
		'timestamp',
		'flags',
		'eventType',
		'cbBlob'
	]
	def read(self, file):
		super(DBEvent, self).read(file)
		self.blob = file.read(self.cbBlob)
	def write(self, file):
		self.cbBlob = len(self.blob)
		super(DBEvent, self).write(file)
		file.write(self.blob)
	def size(self):
		return super(DBEvent, self).size() + len(self.blob)


#
# Event content types
#
class DBEventBlob(DBStruct):
	# Omit Unicode if your event type does not care
	def __init__(self, file = None, unicode = None):
		self.unicode = unicode
		# "file" may be a Bytes() instead
		if file <> None:
			read_op = getattr(file, "read", None)
			if not callable(read_op):
				buf = file
				file = io.BytesIO(file)
			else:
				buf = none
			self.read(file)
			pos = file.tell()
			if buf and not hasattr(self, 'problem') and (len(buf) > pos):
				self.problem = 'Bytes remaining in the buffer ('+str(pos)+' out of '+str(len(buf))+')'
				self.tail = buf[pos:]
	
	def try_read_str(self, file, default = None):
		s = ""
		c = file.read(1)
		if len(c) == 0:
			return default
		while True:
			if c == chr(0):
				break
			s = s + c
			c = file.read(1)
			if len(c) == 0:
				self.problem = 'No more bytes where string is expected in event data (read:'+s.encode('hex')+')'
				break
		if self.unicode:
			return s.decode('utf-8')
		else:
			return unicode(s.decode('mbcs'))
	
	def read_str(self, file):
		ret = self.try_read_str(file)
		if ret == None:
			self.problem = 'String expected but not found in event data'
		return ret

#
# Simple message. May contain arbitrary additional fields
#
class MessageBlob:
	def __init__(self, **kwargs):
		self.__dict__.update(kwargs)

class DBURLBlob(DBEventBlob):
	# include/m_protosvc.h says:
	#   blob contains szMessage without 0 terminator
	# EmLanProto/src/mlan.cpp: UTF message WITH \0
	# FacebookRM/src/process.cpp: UTF message WITH \0
	# ICQCorp/src/services.cpp: TWO ANSI/UTF messages WITH \0 each (message + description)
	def read(self, file):
		super(DBURLBlob, self).read(file)
		self.url = self.read_str(file)
		self.desc = self.try_read_str(file)

class DBContactsBlob(DBEventBlob):
	# Any number of nick:string\0 address:string\0 sequences, where `address` is protocol-specific
	#   ContactsPlus\src\send.cpp
	#   IcqOscarJ\src\icq_proto.cpp\RecvContacts()
	#   MSN\src\msn_proto.cpp\RecvContacts()
	#   MRA\src\MraProto.cpp\RecvContacts()
	#   src\skype_proto.cpp\RecvContacts()
	def read(self, file):
		super(DBURLBlob, self).read(file)
		self.contacts = []
		while True:
			nick = self.try_read_str(file)
			if nick <> None:
				address = self.read_str(file)
			self.contacts.append((nick, address))

class DBAuthBlob(DBEventBlob):
	#"Contact added"
	#[uin:DWORD; hContact:DWORD; nick, firstName, lastName, email: str]
	FORMAT = "=II"
	def unpack(self, tuple):
		(self.uin,
		self.hContact
		) = tuple
	def read(self, file):
		super(DBAuthBlob, self).read(file)
		self.nick = self.read_str(file)
		self.firstName = self.read_str(file)
		self.lastName = self.read_str(file)
		self.email = self.read_str(file)
class DBAuthRequestBlob(DBAuthBlob):
	# AuthRequest adds [reason:str]
	def read(self, file):
		super(DBAuthRequestBlob, self).read(file)
		self.reason = self.read_str(file)

class DBFileBlob(DBEventBlob):
	# DWORD, szFilename\0, [szFilename\0...], szDescription\0
	FORMAT = "=I"
	def unpack(self, tuple):
		(self.header,			# Purpose unknown, typically == 0
		) = tuple
	def read(self, file):
		super(DBFileBlob, self).read(file)
		self.filenames = []
		while True:
			filename = self.try_read_str(file)
			if filename == None:
				break
			self.filenames.append(filename)
		if len(self.filenames) < 2:	# There must be at least one of each
			self.problem = 'No filename or no description in EVENTTYPE_FILE'
		if len(self.filenames) > 0:
			self.description = self.filenames.pop()

class DBAvatarChangeBlob(DBEventBlob):
	# AvatarHistory/src/AvatarHistory.cpp/AvatarChanged
	# Stores a single string, relative path to new avatar.
	def read(self, file):
		super(DBAvatarChangeBlob, self).read(file)
		self.rel_path = self.read_str(file)

class DBICQMissedMessageBlob(DBEventBlob):
	# "This message was blocked by the ICQ server"
	#   IcqOscarJ\src\icqosc_svcs.cpp\icq_getEventTextMissedMessage
	#   IcqOscarJ\src\fam_04message.cpp\handleMissedMsg
	ICQ_REJECTED_BLOCKED		= 0	# The message was invalid.
	ICQ_REJECTED_TOO_LONG		= 1	# The message was too long.
	ICQ_REJECTED_FLOOD			= 2	# The sender has flooded the server.
	ICQ_REJECTED_TOO_EVIL		= 4	# You are too evil.
	# Other codes are not entirely impossible
	reason_text = {
		ICQ_REJECTED_BLOCKED: 'The message was invalid.',
		ICQ_REJECTED_TOO_LONG: 'The message was too long.',
		ICQ_REJECTED_FLOOD: 'The sender has flooded the server.',
		ICQ_REJECTED_TOO_EVIL: 'You are too evil.',
	}
	def format_text(self):
		if self.reason in self.reason_text:
			return self.reason_text[self.reason]
		else:
			return 'Message rejected (ICQ reason code: '+str(self.reason)+')'
	FORMAT = "=H"
	def unpack(self, tuple):
		(self.reason,			# Purpose unknown, typically == 0
		) = tuple
class DBICQSMSBlob(DBEventBlob):
	# Sent or received SMS
	#   plugins\SMS\src\receive.cpp\handleAckSMS()
	#     DBEF_UTF freeform text:
	#     "SMS From: +%phone%\r\n%message%" + (DWORD == 0)
	#   plugins\SMS\src\send.cpp\StartSmsSend()
	#     DBEF_UTF, Freeform text:
	#     "SMS To: +%phone%\r\n%message%" + (DWORD)
	# Strings are not localized so the text is predictable.
	def read(self, file):
		super(DBICQSMSBlob, self).read(file)
		self.text = self.read_str(file)
		self.unk1 = struct.unpack("=I", file.read(4))[0]
class DBICQSMSConfirmationBlob(DBEventBlob):
	# SMS delivery receipt from server
	#   plugins\SMS\src\receive.cpp\handleAckSMS()
	#     "SMS Confirmation From: +%phone%\r\nSMS was sent succesfully", (DWORD == 0)
	#     "SMS Confirmation From: +%phone%\r\nSMS was not sent succesfully: %error%", (DWORD ==0)
	# Strings are not localized so the text is predictable.
	def read(self, file):
		super(DBICQSMSConfirmationBlob, self).read(file)
		self.text = self.read_str(file)
		self.unk1 = struct.unpack("=I", file.read(4))[0]
class DBICQWebPagerBlob(DBEventBlob):
	# IcqOscarJ\src\fam_04message.cpp\handleMessageTypes()
	#   "blob is: body(ASCIIZ), name(ASCIIZ), email(ASCIIZ)"
	#   The strings in fact follow the encoding of the Miranda build (even though they don't set DBEF_UTF8)
	def read(self, file):
		super(DBICQWebPagerBlob, self).read(file)
		self.body = self.read_str(file)
		self.name = self.read_str(file)
		self.email = self.read_str(file)
class DBICQEmailExpressBlob(DBEventBlob):
	# IcqOscarJ\src\fam_04message.cpp\handleMessageTypes()
	# Exactly matches ICQEVENTTYPE_WEBPAGER blob.
	pass

class DBJabberPresenceBlob(DBEventBlob):
	JABBER_DB_EVENT_PRESENCE_SUBSCRIBE		= 1
	JABBER_DB_EVENT_PRESENCE_SUBSCRIBED		= 2
	JABBER_DB_EVENT_PRESENCE_UNSUBSCRIBE	= 3
	JABBER_DB_EVENT_PRESENCE_UNSUBSCRIBED	= 4
	JABBER_DB_EVENT_PRESENCE_ERROR			= 5
	FORMAT = "=B"
	def unpack(self, tuple):
		(self.presence,
		) = tuple
class DBJabberChatStatesBlob(DBEventBlob):
	JABBER_DB_EVENT_CHATSTATES_GONE			= 1
	FORMAT = "=B"
	def unpack(self, tuple):
		(self.state,
		) = tuple

class DBVKontakteUserDeactivateActionBlob(DBEventBlob):
	# Contains a single localized UTF8 string with a description of one of three events:
	#  1. Restored control of their page, 2. Deactivated (deleted), 3. Deactivated (banned)
	# vkontakte\src\misc.cpp
	DESC_RESTORED_CONTROL = 'Restored control of their page'
	DESC_DEACTIVATED = 'Deactivated (deleted)'
	DESC_BANNED = 'Deactivated (banned)'
	def read(self, file):
		super(DBVKontakteUserDeactivateActionBlob, self).read(file)
		self.description = self.read_str(file)


class MirandaDbxMmap(object):
	file = None
	def __init__(self, filename, writeable=False):
		self._baseProtocols = {}
		self._moduleNames = {}
		self._eventChains = {}
		open_mode = "rb+" if writeable else "rb"
		self.file = open(filename, open_mode)
		self.filename = filename
		self.header = self.read(DBHeader())
		self.user = self.read(DBContact(), self.header.ofsUser)
		self.expand_contact(self.user)

	# Reads and unpacks data at a given offset or where the pointer is now
	# cl must provide cl.FORMAT and cl.unpack()
	def read(self, cl, offset = None):
		if offset <> None:
			self.file.seek(offset, 0)
		cl.read(self.file)
		log.debug(vars(cl))
		return cl
	
	# By default we write at the offset specified in the structure
	def write(self, cl, offset):
		log.debug('Writing at offset '+str(offset)+': '+str(vars(cl)))
		self.file.seek(offset, 0)
		cl.write(self.file)
	
	
	# Reserves space of a given size at the end of the file. Returns its offset
	def reserve_space(self, size):
		offset = self.header.ofsFileEnd
		self.header.ofsFileEnd += size
		# If we seek() and write() there, the file will automatically be expanded,
		# but we might exit before that so let's make sure Miranda finds the file correct.
		self.file.seek(0, os.SEEK_END)
		fsize = self.file.tell()
		if fsize < self.header.ofsFileEnd:
			self.file.seek(self.header.ofsFileEnd+4096)
			self.file.write('\0')
		self.write(self.header, 0)
		return offset
	# Reallocates space of a given size, possibly inplace. Returns its new offset
	def realloc_space(self, offset, old_size, new_size):
		if old_size >= new_size:
			if old_size > new_size:
				self.header.slackSpace += (new_size-old_size)
				self.write(self.header, offset=0)
			return offset
		self.header.slackSpace += old_size
		# ^ do not commit, we'll write more in a moment
		return reserve_space(new_size)
	# Releases a chunk of space
	def free_space(self, offset, size):
		self.header.slackSpace += size
		self.write(self.header, 0)


	#
	# Modules and protocols
	#
	def read_module(self, offset):
		return self.read(DBModuleName(), offset)
	# Cached module list
	# All reads and writes to modules must go through cache-aware function
	_modules = None
	def get_modules(self):
		if self._modules == None:
			self._modules = []
			moduleOffset = self.header.ofsModuleNames
			while moduleOffset <> 0:
				module = self.read(DBModuleName(), moduleOffset)
				self._modules.append(module)
				moduleOffset = module.ofsNext
		return self._modules
	def get_module(self, offset):
		for module in self.get_modules():
			if module.offset == offset:
				return module
		return None
	def get_module_name(self, ofsModule):
		module = self.get_module(ofsModule)
		return module.name if module else None
	def find_module_name(self, name):
		for module in self.get_modules():
			if module.name.lower() == name.lower():
				return module.offset
		return None

	# For modules that are accounts, returns their base protocol (string)
	_baseProtocols = None
	def get_base_proto(self, moduleName):
		if isinstance(moduleName, (int, long)):	# this is offset
			moduleName = self.get_module_name(moduleName)
			if moduleName == None: return None
		if not moduleName in self._baseProtocols:
			self._baseProtocols[moduleName] = self.user.get_setting(moduleName, "AM_BaseProto")
		return self._baseProtocols[moduleName]
	
	# Returns ofsModuleName for the newly registered module
	def add_module_name(self, name):
		# Insert new module name
		moduleName = DBModuleName()
		moduleName.name = name
		moduleName.ofsNext = 0
		moduleName.offset = self.reserve_space(moduleName.size())
		self.write(moduleName, moduleName.offset)
		# Update last module name to point to this
		if self.header.ofsModuleNames == 0:
			self.header.ofsModuleNames = moduleName.offset
		else:
			offset = self.header.ofsModuleNames
			while True:
				lastModuleName = self.read(DBModuleName(), offset)
				if lastModuleName.ofsNext == 0:
					break
				offset = lastModuleName.ofsNext
			lastModuleName.ofsNext = moduleName.offset
			self.write(lastModuleName, lastModuleName.offset)

	#
	# Contacts
	#
	
	def read_contact(self, offset):
		return self.read(DBContact(), offset)
	
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
		contact.uin = self.contact_UIN(contact)
	
	# Returns a contact by its database contactID
	def contact_by_id(self, id):
		if id == 0:
			return self.user
		for contact in self.contacts():
			if contact.contactID == id:
				return contact
		return None
	
	# Returns the meta contact for the given contact, or None
	def get_meta_contact(self, contact):
		metaId = contact.get_meta_parent()
		return self.contact_by_id(metaId) if metaId <> None else None
	
	# Returns the contact which hosts events for this contact - the contact itself or its metacontact.
	#   contact: ID or DBContact
	def get_host_contact(self, contact):
		if isinstance(contact, (int, long)):
			contact = self.contact_by_id(contact)
		return self.get_meta_contact(contact) or contact
	
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
			if contact.uin and fnmatch.fnmatch(str(contact.uin).lower(), contact_mask):
				ret.append(contact)
				continue
			if contact.uin and fnmatch.fnmatch((contact.protocol+u'\\'+str(contact.uin)).lower(), contact_mask):
				ret.append(contact)
				continue
			if '#'+str(contact.contactID) == contact_mask:
				ret.append(contact)
				continue
		log.warning('entries: '+str(len(ret)))
		return ret


	# Different protocols use different IDs (UINs, JIDs, Skype/Telegram IDs and so on)
	# We will refer to these as UINs to differentiate from contactIDs
	
	# Returns the protocol-dependent UIN for the contact, or None
	def contact_UIN(self, contact):
		if contact.protocol == None:
			return None
		contact.id = contact.get_setting(contact.protocol, "jid")		# xmpp
		if contact.id == None:
			contact.id = contact.get_setting(contact.protocol, "uin")	# ICQ
		if contact.id == None:
			contact.id = contact.get_setting(contact.protocol, "id")	# vkontakte

	# Returns the "uri:UIN" scheme URI for the contact
	def contact_URI(self, contact = None, proto = None):
		if isinstance(contact, DBContact):
			if proto == None:
				proto = contact.protocol
			uin = self.contact_UIN(self, contact)
		else:
			assert proto <> None
			uin = contact
		if (uin == None) or (proto == None):
			return None
		base_proto = self.get_base_proto(contact.protocol).lower()
		if base_proto == None:
			return None
		scheme = self.proto_uri_scheme(base_proto)
		if scheme == None:
			return None
		return scheme+':'+uin

	def proto_uri_scheme(self, base_proto):
		if base_proto=='jabber':	return 'xmpp'
		if base_proto=='icq':		return 'icq'
		if base_proto=='irc':		return 'irc'
		if base_proto=='telegram':	return 'telegram'
		if base_proto=='skype':		return 'skype'
		return None

	#
	# Events
	#
	def read_event(self, offset):
		return self.read(DBEvent(), offset)
	
	# Adds a new event to the database. Returns new event offset.
	#	contact: Determined automatically, minding metacontacts.
	#	         Only pass this to add event to non-standard event chain.
	#	insert_after: Offset of the event to insert this one after.
	#	  None:	Determine automatically from timestamp
	#	  0:	First event in the chain
	#	  -1:	Last event in the chain
	def add_event(self, event, contact=None, insert_after=None):
		event.offset = self.reserve_space(event.size())
		if contact == None:
			contact = self.get_host_contact(event.contactID)
		# Select insert_after
		if insert_after == None:
			insert_after = self.last_event_before_timestamp(contact, event.timestamp+1)
		elif insert_after == 0:
			insert_after = None
		elif insert_after < 0:
			insert_after = self.db.get_last_event(contact)
		else:
			# Requery the event! The prev/next in this one can be stale
			insert_after = self.read_event(insert_after.offset)
		# Link events together
		if insert_after == None:
			if contact.ofsFirstEvent <> 0:
				evtNext = self.read_event(contact.ofsFirstEvent)
			else:
				evtNext = None
			contact.ofsFirstEvent = event.offset
		elif insert_after.ofsNext <> 0:
			evtNext = self.read_event(insert_after.ofsNext)
		else:
			evtNext = None
			contact.ofsLastEvent = event.offset
		event.ofsPrev = insert_after.offset if insert_after <> None else 0
		event.ofsNext = evtNext.offset if evtNext <> None else 0
		# Write
		self.write(event, event.offset)
		if evtNext <> None:
			evtNext.ofsPrev = event.offset
			self.write(evtNext, evtNext.offset)
		if insert_after <> None:
			insert_after.ofsNext = event.offset
			self.write(insert_after, insert_after.offset)
		contact.eventCount += 1
		self.write(contact, contact.offset)
		# When updating metacontacts, we must update both the host and the child
		if contact.contactID <> event.contactID:
			child_contact = self.contact_by_id(event.contactID)
			child_contact.eventCount += 1
			self.write(child_contact, child_contact.offset)
		self.event_cache_invalidate(contact.contactID)
		return event.offset
	
	# Deletes event from the given contact, linking events around it together
	def delete_event(self, offset, contact=None):
		# We must use base offsets only, clients will often have stale ofsPrev/ofsNext pointers,
		# especially when doing mass deletions.
		if isinstance(offset, DBEvent):
			offset = offset.offset
		log.debug('Deleting event '+str(offset)+'...')
		# To trust DBEvent() fields we must have some kind of "single-instance events":
		# cache returned DBEvents and update on the fly, removing only on __del__.
		# And that might not be wise. What if someone's iterating over them? Safer to just requery on each edit.
		event = self.read_event(offset)
		if contact == None:
			contact = self.get_host_contact(event.contactID)
			# The event can IN FACT be hosted elsewhere, for whatever reason.
			# But there's no quick way to find true host unless told.
		ofsPrev = event.ofsPrev
		ofsNext = event.ofsNext
		# Link events around this one together
		if ofsPrev == 0:
			contact.ofsFirstEvent = ofsNext
			# Do not write out, will modify more
		else:
			evtPrev = self.read_event(ofsPrev)
			evtPrev.ofsNext = ofsNext
			self.write(evtPrev, evtPrev.offset)	# The size shouldn't have changed
		if ofsNext == 0:
			contact.ofsNextEvent = ofsPrev
		else:
			evtNext = self.read_event(ofsNext)
			evtNext.ofsPrev = ofsPrev
			self.write(evtNext, evtNext.offset)	# The size shouldn't have changed
		contact.eventCount -= 1
		self.write(contact, contact.offset)
		# When updating metacontacts, we must update both the host and the child
		if contact.contactID <> event.contactID:
			child_contact = self.contact_by_id(event.contactID)
			child_contact.eventCount -= 1
			self.write(child_contact, child_contact.offset)
		self.free_space(event.offset, event.size())
		self.event_cache_invalidate(contact.contactID)
	
	# MetaContacts steal events from their children but leave contactId and moduleName intact:
	#    Contact1: 4 events, first: None
	#    Contact2: 2 events, first: None
	#    Meta: 8 events, c1 -> c2 -> c1 -> c1 -> c2 -> c1
	# When the contact lacks events we scan through its meta.
	# But EVERY subcontact has to iterate ALL metacontact events. So on first read we build an index.
	
	# For now we keep an index of "all events HOSTED by a particular contact"
	# "Events BELONGING to a particular contact" is harder and not required atm
	_eventChains = None	# contactId -> pair(offset, contactId)
	def event_cache_invalidate(self, contactId):
		if contactId in self._eventChains:
			del self._eventChains[contactId]
	
	class EventIter:
		#  ofsFirst: start with a specific event (normally the contact's first event)
		#  chain: first events are cached. After the cached part ends, enum continues from the last offset,
		# expanding the cached chain.
		#  contactId: skip events unless they belong to the given contact
		def __init__(self, db, ofsFirst=None, chain=None, contactId=None):
			self.db = db
			self.chain = chain
			self.chain_idx = 0
			self.offset = ofsFirst
			self.contactId = contactId
		def __iter__(self):
			return self
		def next(self):
			# Go through the cached part of the chain first
			while self.chain_idx < len(self.chain):
				pair = self.chain[self.chain_idx]
				if pair[0]==0:	# zero-offset means the chain is over
					raise StopIteration()	# before chain_idx+=1, so that we return here every next()
				self.chain_idx += 1
				if (self.contactId==None) or (pair[1]==self.contactId):
					event = self.db.read_event(pair[0])
					event.data = self.db.decode_event_data(event)
					return event
			# No zero-offset means we should continue from the last event
			if self.chain and (len(self.chain)>0):
				last_event = self.db.read_event(self.chain[-1][0])
				self.offset = last_event.ofsNext
				# Otherwise we should've been given contact's ofsFirst
			# Read events one by one
			event = None
			while self.offset <> 0:
				event = self.db.read_event(self.offset)
				if event==None: break		# Should not happen but verify
				if self.chain <> None:
					self.chain.append((self.offset, event.contactID))
					self.chain_idx += 1		# Or we'll return it from the chain next call :)
				self.offset = event.ofsNext
				if (self.contactId == None) or (event.contactID == self.contactId):
					event.data = self.db.decode_event_data(event)
					return event
			# No more events
			if self.chain and (self.chain[-1][0]<>0):
				self.chain.append((0,0))	# Chain terminator
			raise StopIteration()

	def get_event_iter(self, contact, contactId):
		# Event chains for each hoster contact are cached
		chain = self._eventChains.get(contact.contactID, None)
		if chain == None:
			chain = []
			self._eventChains[contact.contactID] = chain
		return self.EventIter(self, contact.ofsFirstEvent, chain, contactId)

	# Retrieves and decodes all events for the contact. Handles MetaContacts transparently.
	#	contact_id: Return only events for this contactId (MetaContacts can host multiple)
	#	with_metacontacts: Locate this contact events in MetaContacts too.
	#		This may be slower than printing out the entire MetaContact once,
	#		because EVERY subcontact will iterate ALL metacontact events, filtering them.
	def get_events(self, contact, with_metacontacts=True, contactId=None):
		# MetaContacts can steal events from their children but leave contactId and moduleName intact
		if (contact.ofsFirstEvent == 0) and (contact.eventCount > 0) and with_metacontacts:
			metaContact = self.get_meta_contact(contact)
			if metaContact <> None:
				contactId2 = contactId if contactId <> None else contact.contactID
				return self.get_event_iter(metaContact, contactId2)
		# If this is a MetaContact itself, skip events not directly owned by it
		if with_metacontacts and (contact.protocol == "MetaContacts") and (contactId == None):
			contactId = contact.contactID
		return self.get_event_iter(contact, contactId)
	
	# Returns the last event in the event chain starting with a given event,
	# or the chain for a given contact
	def get_last_event(self, event):
		if isinstance(event, DBContact):
			event = self.read_event(event.ofsFirstEvent) if event.ofsFirstEvent <> 0 else None
		if event == None:
			return None
		while event.ofsNext <> 0:
			event = self.read_event(event.ofsNext)
		return event
	
	# Returns last event with timestamp < given. For <=, ask for timestamp+1
	def last_event_before_timestamp(self, contact, timestamp, first=None):
		result = None
		for prev_event in self.get_events(contact):
			if prev_event.timestamp>=timestamp: break
			result = prev_event
		return result
	
	# Returns a class that can be vars()ed
	def decode_event_data(self, event):
		_unicode = (event.DBEF_UTF & event.flags) <> 0
		proto = self.get_base_proto(event.ofsModuleName)
		if event.flags & event.DBEF_ENCRYPTED: # Can't decrypt, return hex
			ret = MessageBlob(
				type = 'encrypted',
				hex = event.blob.encode('hex'),
				unicode = _unicode
			)
		elif event.eventType==event.EVENTTYPE_MESSAGE:
			ret = self.decode_event_data_string(event, _unicode)
		elif event.eventType==event.EVENTTYPE_URL:
			ret = DBURLBlob(event.blob, _unicode)
		elif event.eventType==event.EVENTTYPE_CONTACTS:
			ret = DBContactsBlob(event.blob, _unicode)
		elif event.eventType==event.EVENTTYPE_ADDED:
			ret = DBAuthBlob(event.blob, _unicode)
		elif event.eventType==event.EVENTTYPE_AUTHREQUEST:
			ret = DBAuthRequestBlob(event.blob, _unicode)
		elif event.eventType==event.EVENTTYPE_FILE:
			ret = DBFileBlob(event.blob, _unicode)
		elif event.eventType==event.EVENTTYPE_STATUSCHANGE:
			# Both NewXStatusNotify and TabSRMM produce this as UTF8 DBEF_UTF
			# The text is freeform and expects adding nickname at the beginning:
			#	"signed off."
			#	"signed on and is now %s."
			#	"changed status from %s to %s."
			ret = self.decode_event_data_string(event, _unicode)
		elif event.eventType==event.EVENTTYPE_AVATAR_CHANGE:
			ret = DBAvatarChangeBlob(event.blob, _unicode)
		elif (proto=='ICQ') and (event.eventType==event.ICQEVENTTYPE_MISSEDMESSAGE):
			ret = DBICQMissedMessageBlob(event.blob)
		elif (proto=='ICQ') and (event.eventType==event.ICQEVENTTYPE_SMS):
			ret = DBICQSMSBlob(event.blob)
		elif (proto=='ICQ') and (event.eventType==event.ICQEVENTTYPE_SMSCONFIRMATION):
			ret = DBICQSMSConfirmationBlob(event.blob)
		elif (proto=='ICQ') and (event.eventType==event.ICQEVENTTYPE_WEBPAGER):
			ret = DBICQWebPagerBlob(event.blob)
		elif (proto=='ICQ') and (event.eventType==event.ICQEVENTTYPE_EMAILEXPRESS):
			ret = DBICQEmailExpressBlob(event.blob)
		elif (proto=='JABBER') and (event.eventType==event.EVENTTYPE_JABBER_PRESENCE):
			ret = DBJabberPresenceBlob(event.blob)
		elif (proto=='JABBER') and (event.eventType==event.EVENTTYPE_JABBER_CHATSTATES):
			ret = DBJabberChatStatesBlob(event.blob)
		elif (proto=='VKontakte') and (event.eventType==event.VK_USER_DEACTIVATE_ACTION):
			ret = DBVKontakteUserDeactivateActionBlob(event.blob)
		else:
			ret = MessageBlob(
				type = 'unsupported',
				hex = event.blob.encode('hex'),
				unicode = _unicode
			)
		if hasattr(ret, 'problem'):
			clname = ret.__class__.__name__ # or 'Event'
			log.warning(clname+'@'+str(event.offset)+': '+ret.problem)
			ret.hex = event.blob.encode('hex')	# Full message hex for debugging
		return ret
	
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
			# But the common code that added UTF-16 always added EXACTLY twice the ANSI bytes, so that's a pretty good indicator
			# We may also have MORE than that, and we may have accidental exact match for short messages,
			# but we'll ignore both possibilities for now (too rare in practice to study them)
			if (len(tail) > 0) and (len(tail) == 2*len(blob)+2): # +2b non-removed null
	 			# Actual UTF16 string may be shorter due to UTF-16 over-allocation --
	 			# if ANSI had been MBCS and required less than twice the size
	 			(utf16blob, tail) = utfutils.eatutf16(tail)
	 			utf16text = self.utf16trydecode(utf16blob)
	 			if hasattr(utf16text, 'problem'):
	 				ret.utf16_problem = utf16text.problem
	 				if not hasattr(ret, 'problem'):
	 					ret.problem = 'Problem with UTF16 text'
	 				ret.utf16 = utf16blob.encode('hex')
	 			# We can't verify UTF16==ANSI because:
	 			#  * ANSI may not be in our current locale
	 			#  * ANSI can't encode everything Unicode can
	 			# But we can check that at least it's not an obvious failure
	 			elif (len(utf16text.text) < len(ret.text) / 2):
	 				ret.problem = 'UTF16 tail doesn''t match at all'
	 				ret.utf16 = utf16text.text
	 			else:
	 				# Otherwise replace
	 				utf16text.ansi = ret.text
	 				ret = utf16text
	 				# Since the length had been EXACTLY twice the size, on success throw out the tail (see above)
	 				tail = ''
	 	proto = self.get_base_proto(event.ofsModuleName)
	 	if (len(tail) > 0) and (proto=="VKontakte"):
	 		# Modern versions of VKontakte store message IDs as ASCII text
	 		if len(tail) < 10:	# weed out obvious fails
	 			ret.vk_mid = tail
	 			tail = ''
		if len(tail) > 0:
			ret.remainder = tail.encode('hex')
			if not hasattr(ret, 'problem'):
				ret.problem = "Remainder data in event"
		ret.unicode = _unicode
		return ret

	# Decodes MBCS text and verifies that it's not junk
	def mbcstrydecode(self, data):
		ret = MessageBlob()
		try:
			ret.text = data.decode('mbcs')
			ret.mbcs = data.encode('hex') # TODO: remove
		except DecodeError:
			ret.problem = "Cannot decode as mbcs"
			ret.mbcs = data.encode('hex')
			return ret
		return ret

	# Decodes UTF8 text and verifies that it's not junk
	# Returns (True, the decoded text) or (False, hex text, problem description)
	def utf8trydecode(self, data):
		ret = MessageBlob()
		try:
			ret.text = data.decode('utf-8')
		except UnicodeDecodeError:
			ret.problem = "Cannot decode as utf-8"
			ret.utf8 = data.encode('hex')
		return ret
	
	def utf16trydecode(self, data):
		ret = MessageBlob()
		try:
			ret.text = data.decode('UTF-16LE')	# LE, so that it doesn't eat bom, if it's present
			ret.utf16 = data.encode('hex') # TODO: remove
		except UnicodeDecodeError as e:
			ret.problem = "Cannot decode as utf-16: "+str(e)
			ret.utf16 = data.encode('hex')
		return ret


# Can be called manually for testing
def main():
	parser = argparse.ArgumentParser(description="Parse and print Miranda.",
		parents=[coreutils.argparser()])
	parser.add_argument("dbname", help='path to database file')
	parser.add_argument("--write", help='opens the database for writing (WARNING: enables editing functions!)', action='store_true')
	subparsers = parser.add_subparsers(title='subcommands')
	
	sparser = subparsers.add_parser('dump-modules', help='prints all module names')
	sparser.add_argument('offset', type=int, nargs='*', help='print module names from these offsets (default: all by list)')
	sparser.add_argument('--low', action='store_true', help='print low-level info')
	sparser.set_defaults(func=dump_modules)
	
	sparser = subparsers.add_parser('add-module', help='add-module')
	sparser.add_argument('module-name', type=str, nargs='+', help='add module with this name')
	sparser.set_defaults(func=add_module)
	
	sparser = subparsers.add_parser('dump-contacts', help='prints contacts')
	sparser.add_argument('contact', type=str, nargs='*', help='print these contacts (default: all)')
	sparser.add_argument("--low", action='store_true', help='prints low-level contact info')
	sparser.set_defaults(func=dump_contacts)
	
	sparser = subparsers.add_parser('dump-settings', help='prints settings for the given contact')
	sparser.add_argument('contact', type=str, nargs='*', help='print settings for these contacts (default: all)')
	sparser.set_defaults(func=dump_settings)
	
	sparser = subparsers.add_parser('event-stats', help='collects event statistics')
	sparser.set_defaults(func=event_stats)
	
	sparser = subparsers.add_parser('dump-events', help='prints all events for the given contacts')
	sparser.add_argument('contact', type=str, nargs='*', help='print events for these contacts')
	sparser.add_argument("--nometa", help='dumps events attached to this contact but not belonging to it', action='store_true')
	sparser.add_argument("--bad", help='dumps only bad events', action='store_true')
	sparser.add_argument("--unsupported", help='dumps only unsupported events', action='store_true')
	sparser.add_argument("--low", help='print low-level info', action='store_true')
	sparser.set_defaults(func=dump_events)

	sparser = subparsers.add_parser('dump-event', help='prints the specific events')
	sparser.add_argument('offset', type=int, nargs='+', help='print events at these offsets')
	sparser.add_argument("--low", help='print low-level info', action='store_true')
	sparser.set_defaults(func=dump_event)

	sparser = subparsers.add_parser('add-event', help='adds a simple message event to the end of the chain')
	sparser.add_argument('--contact', type=int, required=True, metavar='contact id')
	sparser.add_argument('--text', type=str, required=True, metavar='event text')
	sparser.add_argument('--module', type=str, metavar='module name (default: contact\'s proto)')
	sparser.add_argument('--timestamp', type=int, metavar='timestamp for the event (default: now)')
	sparser.add_argument('--after', type=int, metavar='event offset', help='insert this event after this one in the chain (0: first; -1: last; default: according to timestamp)')
	sparser.set_defaults(func=add_event)

	sparser = subparsers.add_parser('delete-event', help='deletes event at a given offset')
	sparser.add_argument('offset', type=int, nargs='+', help='offset to delete an event at')
	sparser.set_defaults(func=delete_event)

	args = parser.parse_args()
	coreutils.init(args)
	
	db = MirandaDbxMmap(args.dbname, writeable=args.write)
	
	if args.func <> None:
		args.func(db, args)


def dump_modules(db, args):
	if args.offset:
		list = []
		for offset in args.offset:
			list.append(db.read_module(offset))
	else:
		list = db.get_modules()
	for module in list:
		if args.low:
			print str(vars(module))
		else:
			print "Module: "+module.name

def add_module(db, args):
	for module_name in args.module_name:
		db.add_module_name(module_name)


# Returns all contacts in the database
def all_contacts(db):
	return ([db.user] + db.contacts())

# Selects all contacts matching any pattern in the list
def select_contacts(db, list):
	ret = []
	for contact_name in list:
		ret += db.contacts_by_mask(contact_name)	# Too lazy to weed out duplicates atm
	return ret

# Selects all contacts matching any pattern in the list, or all contacts if the list is not given
def select_contacts_opt(db, list):
	return select_contacts(db, list) if list else all_contacts(db)

def dump_contacts(db, args):
	totalEvents = 0
	for contact in select_contacts_opt(db, args.contact):
		if args.low:
			pprint.pprint(vars(contact))
			continue
		print unicode(contact.display_name)
		print u"  Protocol: "+unicode(contact.protocol)
		print u"  UIN: "+unicode(contact.uin)
		print u"  Contact ID: #"+unicode(contact.contactID)
		print u"  Nick: "+unicode(contact.nick)
		print u"  MyHandle: "+unicode(contact.get_setting('CList', 'MyHandle'))
		print u"  Group: "+unicode(contact.get_setting('CList', 'Group'))
		print u"  Hidden: "+unicode(contact.get_setting('CList', 'Hidden'))
		print u"  Events: "+unicode(contact.eventCount)
		totalEvents += contact.eventCount
	print "Total events: "+unicode(totalEvents)

def dump_settings(db, args):
	for contact in select_contacts_opt(db, args.contact):
		display_name = ''
		if hasattr(contact, 'display_name') and contact.display_name:
			display_name = unicode(contact.display_name)
		if hasattr(contact, 'protocol') and contact.protocol:
			display_name += ' ('+contact.protocol+')'
		print display_name
		for name in contact.settings:
			print unicode(contact.settings[name])


def event_stats(db, args):
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
		moduleProto = db.get_base_proto(moduleName)
		if moduleProto <> None:
			moduleName = moduleProto
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

def dump_event(db, args):
	for offset in args.offset:
		event = db.read_event(offset)
		if args.low:
			print str(vars(event))
		else:
			print format_event(db, event)

def dump_events(db, args):
	def should_print_event(event):
		if args.bad and hasattr(data, 'problem'):
			return True
		if args.unsupported and (getattr(data, 'type', None) in ['unsupported', 'encrypted']):
			return True
		return not (args.bad or args.unsupported)
	for contact in select_contacts_opt(db, args.contact):
		print "Events for "+contact.display_name+": "
		for event in db.get_events(contact, with_metacontacts=not (args.nometa)):
			data = event.data
			if hasattr(data, 'problem'):
				data.offset = event.offset
			if not should_print_event(event):
				continue
			if args.low:
				print str(vars(event))
			else:
				print format_event(db, event, data)

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
	return str(event.offset) + " " + str(event.timestamp) + " " + db.get_module_name(event.ofsModuleName) + " " + str(event.eventType) + " " + str(event.flags) + " " + data


def add_event(db, args):
	contact = db.contact_by_id(args.contact)
	if contact == None:
		raise Exception("Contact not found with ID: "+str(args.contact))
	event = DBEvent()
	event.contactID = args.contact
	if not args.module:
		args.module = contact.protocol
		if not args.module:
			raise Exception('Contact has no protocol, please specify module name')
	event.ofsModuleName = db.find_module_name(args.module)
	if event.ofsModuleName == None:
		raise Exception("Module not found: "+args.module)
	if args.timestamp:
		event.timestamp = args.timestamp
	else:
		event.timestamp = calendar.timegm(datetime.now().timetuple())
	if (args.after <> None) and (args.after > 0):
		insert_after = db.read_event(args.after)	# verify that it's an event
	event.flags = event.DBEF_UTF
	event.eventType = 0
	event.blob = args.text.encode('utf-8')
	db.add_event(event, contact, insert_after=args.after)

def delete_event(db, args):
	for offset in args.offset:
		db.delete_event(offset)	# Will verify that it's an event

if __name__ == "__main__":
	sys.exit(main())
