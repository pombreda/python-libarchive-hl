#
# Author: Stanislaw Adaszewski, 2014
# Email: s.adaszewski@gmail.com
# http://algoholic.eu
#


from libarchive import Archive, Entry
import os
from collections import defaultdict


def _mkdd():
	return defaultdict(_mkdd)


class AttributeManager(object):
	def __init__(self, entity):
		pass

	def __iter__(self):
		raise NotImplementedError()

	def __contains__(self, item):
		raise NotImplementedError()

	def __getitem__(self, item):
		raise NotImplementedError()

	def __setitem__(self, key, value):
		raise NotImplementedError()

	def __delitem__(self, key):
		raise NotImplementedError()

	def keys(self):
		raise NotImplementedError()

	def values(self):
		raise NotImplementedError()

	def items(self):
		raise NotImplementedError()

	def iterkeys(self):
		raise NotImplementedError()

	def itervalues(self):
		raise NotImplementedError()

	def iteritems(self):
		raise NotImplementedError()

	def get(self, name, default=None):
		raise NotImplementedError()

	def create(self, name, data, shape=None, dtype=None):
		raise NotImplementedError()

	def modify(self, name, value):
		raise NotImplementedError()


class SoftLink(object):
	def __init__(self, path):
		self.path = path


class ExternalLink(object):
	def __init__(self, filename, path):
		self.filename = filename
		self.path = path


class Group(object):
	def __init__(self, file_, name, parent):
		self.attrs = AttributeManager(self)
		self.file_ = file_
		self.name = name
		self.parent = parent
		self.pathname = parent.pathname + '/' + name
		self._toc = parent._toc[name]

	def __iter__(self):
		raise NotImplementedError()

	def __contains__(self, item):
		raise NotImplementedError()

	def __getitem__(self, item):
		raise NotImplementedError()

	def __setitem__(self, key, value):
		raise NotImplementedError()

	def keys(self):
		raise NotImplementedError()

	def values(self):
		raise NotImplementedError()

	def items(self):
		raise NotImplementedError()

	def iterkeys(self):
		raise NotImplementedError()

	def itervalues(self):
		raise NotImplementedError()

	def iteritems(self):
		raise NotImplementedError()

	def get(self, name, default=None, getclass=False, getlink=False):
		raise NotImplementedError()

	def visit(self, callable):
		raise NotImplementedError()

	def visititems(self, callable):
		raise NotImplementedError()

	def move(self, source, dest):
		raise NotImplementedError()

	def copy(self, source, dest, name=None, shallow=False, expand_soft=False, expand_external=False, expand_refs=False, without_attrs=False):
		raise NotImplementedError()

	def create_group(self, name):
		if name in self._toc:
			raise ValueError('Entry already exists: %s' % name)

		self._toc[name] = _mkdd()

		return Group(self.file_, name, self)

	def require_group(self, name):
		if name in self._toc and self._toc[name] is Group:
			return Group(self.file_, name, self)
		else:
			return self.create_group(name)

	def create_dataset(self, name, shape=None, dtype=None, data=None, **kwargs):
		if name in self._toc:
			raise ValueError('Entry already exists: %s' % name)

		self._toc[name] = Dataset(self.file_, name, self, shape=shape, dtype=dtype, data=data)

	def require_dataset(self, name, shape=None, dtype=None, data=None, **kwargs):
		if name in self._toc and self._toc[name] is Dataset:
			return self._toc[name]
		else:
			return self.create_dataset(name, shape=shape, dtype=dtype, data=data, **kwargs)


class File(Group):
	def __init__(self, filename, mode):
		self.pathname = ''

		f = Archive(filename, 'r')

		self._toc = _mkdd()

		print 'Reading TOC...'
		p = self._toc
		for ent in f:
			frags = filter(lambda x: x != '', ent.pathname.split('/'))
			for frag in frags[:-1]:
				p = p[frag]

			p[frags[-1]] = Entry()

		f.close()

		super(File, self).__init__(self, '/', self) # The / group
		self.filename = filename
		self.mode = mode

	def close(self):
		self._archive.close()

	def flush(self):
		f = self._archive.f
		if f.mode != 'r' and f.mode != 'rb':
			f.flush()
			os.fsync(f.fileno())


class Dataset(object):
	def __init__(self, file_, name, parent):
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
		self.file_ = file_
		self.parent = parent

	def __getitem__(self, item):
		raise NotImplementedError()

	def __setitem__(self, key, value):
		raise NotImplementedError()

	def read_direct(self, array, source_sel=None, dest_sel=None):
		raise NotImplementedError()

	def astype(self, dtype):
		raise NotImplementedError()

	def resize(self, size, axis=None):
		raise NotImplementedError()

	def len(self):
		raise NotImplementedError()
