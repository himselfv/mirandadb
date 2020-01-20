# -*- coding: utf-8 -*-
import sys, os
import argparse
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
