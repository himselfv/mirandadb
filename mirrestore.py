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

def dump_events(db, args):
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


# Main
parser = argparse.ArgumentParser(description="Analyzes Miranda database for corruption.",
	parents=[coreutils.argparser()])
parser.add_argument("dbname", help='path to database file')
subparsers = parser.add_subparsers(title='subcommands')

sparser = subparsers.add_parser('dump-events', help='prints all events for the given contacts')
sparser.add_argument('contact', type=str, nargs='*', help='print events for these contacts')
sparser.add_argument("--bad", help='dumps only bad events', action='store_true')
sparser.add_argument("--bad-offsets", help='gathers bad event offset statistics', action='store_true')
sparser.set_defaults(func=dump_events)

sparser = subparsers.add_parser('restore-events', help='restores events which differ from another DB')

args = parser.parse_args()
coreutils.init(args)

db = MirandaDbxMmapChk(args.dbname)
	
if args.func <> None:
	args.func(db, args)

