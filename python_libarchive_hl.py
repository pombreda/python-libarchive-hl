#
# Author: Stanislaw Adaszewski, 2014
# Email: s.adaszewski@gmail.com
# http://algoholic.eu
#


from libarchive import Archive
import os
from collections import defaultdict


class AttributeManager(object):
	def __init__(self, entity):
		pass

	def __iter__(self):
		pass

	def __contains__(self, item):
		pass

	def __getitem__(self, item):
		pass

	def __setitem__(self, key, value):
		pass

	def __delitem__(self, key):
		pass

	def keys(self):
		pass

	def values(self):
		pass

	def items(self):
		pass

	def iterkeys(self):
		pass

	def itervalues(self):
		pass

	def iteritems(self):
		pass

	def get(self, name, default=None):
		pass

	def create(self, name, data, shape=None, dtype=None):
		pass

	def modify(self, name, value):
		pass


class SoftLink(object):
	def __init__(self, path):
		self.path = path


class ExternalLink(object):
	def __init__(self, filename, path):
		self.filename = filename
		self.path = path


class Group(object):
	def __init__(self, file, name, parent):
		self.attrs = AttributeManager(self)
		self.file = file
		self.name = name
		self.parent = parent

	def __iter__(self):
		pass

	def __contains__(self, item):
		pass

	def __getitem__(self, item):
		pass

	def __setitem__(self, key, value):
		pass

	def keys(self):
		pass

	def values(self):
		pass

	def items(self):
		pass

	def iterkeys(self):
		pass

	def itervalues(self):
		pass

	def iteritems(self):
		pass

	def get(self, name, default=None, getclass=False, getlink=False):
		pass

	def visit(self, callable):
		pass

	def visititems(self, callable):
		pass

	def move(self, source, dest):
		pass

	def copy(self, source, dest, name=None, shallow=False, expand_soft=False, expand_external=False, expand_refs=False, without_attrs=False):
		pass

	def create_group(self, name):
		pass

	def require_group(self, name):
		pass

	def create_dataset(self, shape=None, dtype=None, data=None, **kwargs):
		pass

	def require_dataset(self, shape=None, dtype=None, data=None, **kwargs):
		pass


class File(Group):
	def __init__(self, name, mode):
		super(File, self).__init__(self, '/', None) # The / group

		def mkdd():
			return defaultdict(mkdd)

		self._toc = defaultdict(mkdd)

		print 'Reading TOC...'
		f = Archive(name, 'r')
		p = self._toc['/']
		for ent in f:
			print ent.pathname
			frags = filter(lambda x: x != '', ent.pathname.split('/'))
			for frag in frags[:-1]:
				p = p[frag]

			ent.pathname = ''
			p[frags[-1]] = ent

		print p

		f.close()

		self._archive = Archive(name, mode)
		self.filename = name
		self.mode = mode

	def close(self):
		self._archive.close()

	def flush(self):
		f = self._archive.f
		if f.mode != 'r' and f.mode != 'rb':
			f.flush()
			os.fsync(f.fileno())


class Dataset(object):
	def __init__(self, file, parent, name):
		self.shape = None
		self.dtype = None
		self.size = None
		self.maxshape = None
		self.chunks = None
		self.compression = None
		self.compression_opts = None
		self.scaleoffset = None
		self.shuffle = False
		self.fletcher32 = False
		self.fillvalue = None
		self.dims = None
		self.attrs = AttributeManager(self)
		self.name = ''
		self.file = file
		self.parent = parent

	def __getitem__(self, item):
		pass

	def __setitem__(self, key, value):
		pass

	def read_direct(self, array, source_sel=None, dest_sel=None):
		pass

	def astype(self, dtype):
		pass

	def resize(self, size, axis=None):
		pass

	def len(self):
		pass
