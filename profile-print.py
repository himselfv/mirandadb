# -*- coding: utf-8 -*-
import sys
import pstats

fname = sys.argv[1] if len(sys.argv) >= 2 else 'profile.dat'
fsort = sys.argv[2] if len(sys.argv) >= 3 else 'tottime'

stats = pstats.Stats(fname)
stats.sort_stats(fsort)
stats.print_stats()