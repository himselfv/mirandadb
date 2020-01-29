# -*- coding: utf-8 -*-
import sys, os
import argparse, textwrap
import logging
import codecs

# We need to explicitly set an encoding
#   locale.getpreferredencoding()	# usually CP_ANSI
#   sys.getdefaultencoding()		# usually always 'ascii'
#   sys.stdin.encoding				# usually CP_OEM or whatever is CHCPd! Just what we need.
# Python ignores this documented explicit override but we'll suport it too
if 'PYTHONIOENCODING' in os.environ:
	encoding = os.environ['PYTHONIOENCODING']
else:
	encoding = sys.stdin.encoding
sys.stdout = codecs.getwriter(encoding)(sys.stdout)
sys.stdout.errors = 'replace'					# skip unencodable symbols

def argparser():
	parser = argparse.ArgumentParser(add_help=False)
	parser.add_argument('--debug', action='store_const', const=logging.DEBUG, default=logging.WARNING,
		help='enable debug output')
	return parser

def init(args):
	logging.basicConfig(level=args.debug, format='%(levelname)-8s %(message)s')

# Used with RawDescriptionTextFormatter
# Formats multi-line description text nicely (removes indent, wraps lines)
def format_desc(text):
	text = textwrap.dedent(text)
	text = textwrap.fill(text)
	return text

# A replacement for argparse HelpFormatter which preserves linefeeds when the string starts with one (docstring)
class SmartFormatter(argparse.HelpFormatter):
	def _fill_text(self, text, width, indent):
		if not text.startswith('\n'):
			return super(SmartFormatter, self).__fill_text(text, width, indent)
		# Format in a normal way, but line by line
		outp = ''
		for line in textwrap.dedent(text).strip('\n').splitlines():
			outp += '\n' + textwrap.fill(line, width, initial_indent=indent, subsequent_indent=indent)
		return outp
