# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import coreutils
import mirandadb
import utfutils

log = logging.getLogger('miranda-dbx_mmap')

# Enhances MirandaDbxMmap with some data scan/restore capabilities
class MirandaDbxMmapChk(MirandaDbxMmap):
	def utf8trydecode(self, data):
		ret = super(MirandDbxMmapChk, self).utf8trydecode(data)
		if 'problem' in ret:
			return ret
		
		# Verify that the text looks like valid UTF16 after decoding
		test = utfutils.utf16test(ret['text'])
		if test == True:
			return ret
		ret['problem'] = test
		ret['utf8'] = data.encode('hex')
		text_bytes = utfutils.utf16bytes(ret['text'])
		ret['utf16'] = text_bytes.encode('hex')
		ret['text'] = None # remove text to indicate problems
		
		"""
		# There are some cases where DECODED utf16 contains utf8!
		# Let's try to analyze this
		try:
			# This may again end with \0
			text_bytes = utfutils.removeterm0(text_bytes)
			text2 = text2_bytes.decode('utf-8')
		except UnicodeDecodeError as err:
			text2 = "Doubly decode failed: "+str(err)
		else:
			ret2 = utfutils.utf16test(text2)
			if ret2 == True:
				return (False, text2, 'Doubly encoded utf8!')
		# Doesn't seem to be the case; just return the original attempt
		"""
		return ret

parser = argparse.ArgumentParser(description="Parse and print Miranda.",
	parents=[coreutils.argparser()])
parser.add_argument("dbname", help='path to database file')
parser.add_argument("--dump-events", help='prints all events for the given contact', type=str, action='append')
parser.add_argument("--bad-events", help='dumps bad events only', action='store_true')
parser.add_argument("--bad-offsets", help='gathers bad event offset statistics', action='store_true')
args = parser.parse_args()
coreutils.init(args)

db = MirandaDbxMmapChk(args.dbname)

bad_event_count = 0
bad_offsets = {}		# Bad event offset statistics

if args.dump_events:
	for contact_name in args.dump_events:
		for contact in db.contacts_by_name(contact_name):
			dump_events(db, contact)
	log.warning("Bad entries: "+str(bad_count))
	print "Bad entries:"+str(bad_count)
	if args.bad_offsets:
		print "Bad offsets:"
		print '\n'.join([ repr(key) + ': ' + repr(value) for (key, value) in bad_offsets.items()])


def dump_events(db, contact, params):
	print "Events for "+contact.display_name+": "
	ofsEvent = contact.ofsFirstEvent
	while ofsEvent <> 0:
		event = db.read(DBEvent(), ofsEvent)
		ofsEvent = event.ofsNext
		data = db.decode_event_data(event)
		if isinstance(data, dict) and ('problem' in data):
			bad_event_count += 1
		if args.bad_events:
			if not isinstance(data, dict):
				continue
			if not ('problem' in data):
				continue
		if args.bad_offsets:
			data['offset'] = event.offset
			bad_offset = event.offset // 0x10000
			if bad_offset in bad_offsets:
				bad_offsets[bad_offset] += 1
			else:
				bad_offsets[bad_offset] = 1
		# Stringify data
		if isinstance(data, basestring):
			pass
		elif isinstance(data, dict):
			data = ', '.join([ repr(key) + ': ' + repr(value) for (key, value) in data.items()])
		else:
			data = unicode(vars(data))
		print str(event.timestamp) + " " + db.get_module_name(event.ofsModuleName) + " " + str(event.eventType) + " " + str(event.flags) + " " + data
