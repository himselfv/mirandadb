# -*- coding: utf-8 -*-
import sys
import logging
import argparse

log = logging.getLogger('utf')

def removeterm0(utf_str):
	if utf_str[-1:] == "\0":
		return utf_str[:-1]
	else:
		return utf_str

# Converts u'' utf16 string (a sequence of 2-byte characters), splitting each character into 2 bytes
def utf16bytes(str):
	ret = ""
	for ch in str:
		ch = ord(ch)
		ret += (chr(ch & 0xFF) + chr(ch >> 8))
	return ret

def bytesutf16(bytes):
	ret = ""
	i = 0
	while i < len(bytes) // 2:
		ch1 = bytes[2*i+0]
		ch2 = bytes[2*i+1]
		i += 1
		ret += unichr((ord(ch2) << 8) + ord(ch1))
	if len(bytes) % 2 <> 0:
		log.warning('Odd-length utf16 hex string!')
	return ret


# Verifies that the data looks like valid UTF8 and not junk
# Returns True or a problem description
def utf8test(data):
	# See:
	#   https://www.fileformat.info/info/unicode/utf8.htm
	#   https://stackoverflow.com/a/6240184
	i = 0
	stats = CharStats()
	while i < len(data):
		# First char
		ch1 = ord(data[i])
		i += 1
		if 0x00 <= ch1 <= 0x7F:
			stats.test_char(ch1)
			continue
		if 0xC2 <= ch1 <= 0xDF:
			seqlen = 2
		elif 0xE0 <= ch1 <= 0xEF:
			seqlen = 3
		elif 0xF0 <= ch1 <= oxFF:
			seqlen = 4
		if i + seqlen-2 >= len(data):
			return "Missing surrogate chars"
		
		# Second char
		ch2 = ord(data[i])
		i += 1
		if not (0x80 <= ch2 <= 0xBF):
			return "Bad surrogate char (2)"
		if seq == 2:
			ch = (ch1 & 0xE0) << 6 + (ch2 & 0xC0)
			stats.test_char(ch)
			continue
		
		# Third char
		ch3 = ord(data[i])
		i += 1
		if not (0x80 <= ch3 <= 0xBF):
			return "Bad surrogate char (3)"
		if seq == 3:
			ch = (ch1 & 0xF0) << 12 + (ch2 & 0xC0) << 6 + (ch3 & 0xC0)
			stats.test_char(stats)
			continue
		
		# Fourth char
		ch4 = ord(data[i])
		i += 1
		if not (0x80 <= ch4 <= 0xBF):
			return "Bad surrogate char (4)"
		ch = (ch1 & 0xF8) << 18 + (ch2 & 0xC0) << 12 + (ch3 & 0xC0) << 6 + (ch4 & 0xC0)
		stats.test_char(ch)
	return stats.summarize()


# Verifies that the data looks like valid UTF16 and not junk
# Returns True or a problem description
def utf16test(data):
	i = 0
	stats = CharStats()
	while i < len(data):
		ch = ord(data[i])
		i += 1
		if 0xDC00 <= ch <= 0xDFFF:		# Unpaired low surrogate, clearly a problem
			return "Unpaired low surrogate"
		if 0xD800 <= ch <= 0xDBFF:		# High surrogate
			if i >= len(data):
				return "Unpaired high surrogate"
			ch2 = ord(data[i])
			if not (0xDC00 <= ch2 <= 0xDFFF):			# High surrogate must be followed by low surrogate
				return "Badly paired high surrogate"
			i += 1
			#SURROGATE_OFFSET = 0x10000 - (0xD800 << 10) - 0xDC00
			#codepoint = (ch << 10) + ch2 + SURROGATE_OFFSET
			codepoint = 0x10000 + ((ch - 0xD800) << 10) + (ch2 - 0xDC00)
			stats.test_char(codepoint)
			continue
		stats.test_char(ch)
	return stats.summarize()


# Analyzes characters (Unicode 32 bit codepoints) and tries to estimate the likelihood
# that this is a valid unicode string and not junk
class CharStats:
	def __init__(self):
		self.total = 0		# Total characters
		self.uncommon = 0	# Uncommon characters
		self.rare = 0		# Rare characters
		self.weird_list = []	# Impossible or extremely rare characters

	def summarize(self):
		if len(self.weird_list) > 0:
			return "Bad characters: "+', '.join(self.weird_list)
		elif (self.total < 3):
			if (self.rare >= self.total):
				return "Many rare characters ("+str(self.rare)+' out of '+str(self.total)+')'
			else:
				return True
		elif self.rare*10+self.uncommon >= self.total-self.rare-self.uncommon:
			return "Many rare characters ("+str(self.rare)+', '+str(self.uncommon)+' out of '+str(self.total)+')'
		else:
			return True

	# 0 = normal
	# 1 = uncommon
	# 2 = rare
	# 5 = unassigned
	blocks = [
		(0x00ff, 0),			# Latin
		(0x036f, 1),			# Rare latin, IPA, Bopomofo, Misc
		(0x03ff, 1),			# Greek and Coptic
		(0x04ff, 0),			# Cyrillic
		(0x052f, 2),			# Cyrillic supplement
		(0x1fff, 2),			# A lot of weird scripts
		(0x206f, 0),			# General punctuation
		(0x23ff, 1),			# Misc punctuation
		(0x2fff, 2),			# Misc rare punctuation and scripts
		(0x30ff, 0),			# Kana and kana punctuation
		(0x33ff, 1),			# Bopomofo and other stuff
		(0x4dbf, 1),			# Rarer CJK A
		(0x4dff, 2),			# Weird symbols
		(0x9fff, 0),			# Base CJK
		(0x97ff, 2),			# Weird scripts
		(0xf8ff, 5),			# Surrogates and Private use area
		(0xfeff, 2),			# Weird scripts
		(0xffff, 1),			# Half- and full-width forms
		(0x1eeff, 2),		# Very weird scripts
		(0x1faff, 1),		# Smilies and other pictograms (some are rare, some common, averaged for now)
		(0x2fa1f, 2),		# Very rare CJK
		(0xfffff, 5),		# Private use or invalid
	]

	# Incorporates unicode character (int32) into statistics
	def test_char(self, ch):
		self.total += 1
		found = False
		#log.warning('eh')
		for key in self.blocks:
			#log.warning(key)
			if ch < key[0]:
				val = key[1]
				if val == 0:
					pass
				elif val == 1:
					self.uncommon += 1
				elif val == 2:
					self.rare += 1
				else:
					self.weird_list.append(hex(ch))
				found = True
				break
		if not found:
			self.weird_list.append(hex(ch))
		
		
		"""
		if 0x0000 <= ch <= 0x024f:		# Latin + Extensions
			if not (0x0020 <= ch <= 0x007f):
				self.rare += 1
			return
		if 0x0400 <= ch <= 0x052f:		# Cyrillic + Extensions
			if not (0x0400 <= ch <= 0x04ff):
				self.rare += 1
			return
		if 0x2000 <= ch <= 0x2BFF:		# Punctuation and misc technical
			if 0x206f <= ch:
				self.rare += 1
			return
		if 0x3000 <= ch <= 0x4dbf:		# Kana + CJK Extensions
			if 0x3100 <= ch:
				self.rare += 1
			return
		if 0x4e00 <= ch <= 0x9fff:		# CJK characters
			self.rare += 1
			return
		if 0xfe00 <= ch <= 0xffff:		# Half-width characters and misc
			self.rare += 1
			return
		if 0x1F300 <= ch <= 0x1F64F:	# Emoji and pictographs
			return
		# Otherwise it's a really weird or undefined character
		self.weird += 1
		self.weird_list.append(hex(ch))
		"""


# Can be called manually for testing
def main():
	parser = argparse.ArgumentParser(description="Parse and print Miranda.")
	parser.add_argument('--debug', action='store_const', const=logging.DEBUG, default=logging.WARNING,
		help='enable debug output')
	parser.add_argument("--test-file", help='evaluates all lines from the file', type=str)
	args = parser.parse_args()
	
	logging.basicConfig(level=args.debug, format='%(levelname)-8s %(message)s')
	
	if args.test_file:
		test_file_hex(args.test_file)

def test_file_hex(filename):
	for line in open(filename, 'r'):
		line = line.strip('\n\r')
		print line
		utf16line = bytesutf16(line.decode('hex'))
		print str(utf16test(utf16line))
		print ""

if __name__ == "__main__":
	sys.exit(main())
