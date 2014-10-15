#
# Author: Stanislaw Adaszewski, 2014
# Email: s.adaszewski@gmail.com
# http://algoholic.eu
#


from libarchive import SeekableArchive, Archive, Entry
import os
from collections import defaultdict
import numpy as np
import time
import stat
from StringIO import StringIO


class AttributeManager(dict):
	def __init__(self, entity):
		super(AttributeManager, self).__init__()
		self._entity = entity

	def __setitem__(self, key, value):
		super(AttributeManager, self).__setitem__(key, value)
		self._entity._touched = True

	def __delitem__(self, key):
		super(AttributeManager, self).__delitem__(key)
		self._entity._touched = True

	def create(self, name, data, shape=None, dtype=None):
		if name in self:
			raise ValueError('Key already exists: %s' % name)
		self[name] = np.array(data, dtype=dtype)
		if not shape is None:
			self[name] = self[name].reshape(shape)
		self._entity._touched = True

	def modify(self, name, value):
		if not name in self:
			raise ValueError('Key doesn''t exist: %s' % name)
		self[name] = value
		self._entity._touched = True


class SoftLink(object):
	def __init__(self, file_, path):
		self.file_ = file_
		self.path = path


class Group(object):
	def __init__(self, file_, entry_name, parent):
		self.attrs = AttributeManager(self)
		self.file_ = file_
		self.entry_name = entry_name
		self.parent = parent
		self.name = parent.name + ('/' if parent.name == '' or parent.name[-1] != '/' else '') + entry_name
		self._toc = {}
		self._touched = True

	def __iter__(self):
		return iter(self._toc)

	def __contains__(self, item):
		try:
			self.find(item)
		except KeyError:
			return False

	def __getitem__(self, item):
		return self.find(item)

	def __setitem__(self, key, value):
		raise NotImplementedError()

	def __delitem__(self, key):
		raise NotImplementedError()

	def keys(self):
		return self._toc.keys()

	def values(self):
		return self._toc.values()

	def items(self):
		return self._toc.items()

	def iterkeys(self):
		return self._toc.iterkeys()

	def itervalues(self):
		return self._toc.itervalues()

	def iteritems(self):
		return self._toc.iteritems()

	def get(self, name, default=None, getclass=False, getlink=False):
		try:
			return self.find(name)
		except KeyError:
			return default

	def visit(self, callable):
		for (k, v) in self._toc.iteritems():
			callable(v.name)

	def visititems(self, callable):
		for (k, v) in self._toc.iteritems():
			callable(v.name, v)

	def move(self, source, dest):
		raise NotImplementedError()

	def copy(self, source, dest, name=None, shallow=False, expand_soft=False, expand_external=False, expand_refs=False, without_attrs=False):
		raise NotImplementedError()

	def find(self, entry_name):
		frags = filter(lambda x: x != '', entry_name.split('/'))

		g = self
		for frag in frags:
			if not frag in g._toc:
				raise KeyError('Entry doesn\'t exist: %s' % entry_name)
			g = g._toc[frag]

		return g

	def create_group(self, entry_name, _already_exists=None):
		if entry_name == '':
			return self

		frags = filter(lambda x: x != '', entry_name.split('/'))

		g = self
		for frag in frags[:-1]:
			if not frag in g._toc:
				# print 'Creating group', frag, 'in', g.name
				g._toc[frag] = Group(self.file_, frag, g)
			g = g._toc[frag]

		if frags[-1] in g._toc:
			if not _already_exists is None:
				_already_exists[0] = g._toc[frags[-1]]
			raise ValueError('Entry already exists: %s' % entry_name)

		# print 'Creating group', frags[-1], 'in', g.name
		g._toc[frags[-1]] = Group(self.file_, frags[-1], g)

		return g._toc[frags[-1]]

	def require_group(self, entry_name):
		alrdy = [None]
		try:
			return self.create_group(entry_name, _already_exists=alrdy)
		except ValueError:
			return alrdy[0]

	def create_dataset(self, entry_name, shape=None, dtype=None, data=None, _already_exists=None, **kwargs):
		frags = filter(lambda x: x != '', entry_name.split('/'))

		g = self.require_group('/'.join(frags[:-1]))

		if frags[-1] in g._toc:
			if not _already_exists is None:
				_already_exists[0] = g._toc[frags[-1]]
			raise ValueError('Entry already exists: %s' % entry_name)

		g._toc[frags[-1]] = Dataset(self.file_, frags[-1], g, shape=shape, dtype=dtype, data=data)

	def require_dataset(self, name, shape=None, dtype=None, data=None, **kwargs):
		alrdy = [None]
		try:
			return self.create_dataset(name, shape=shape, dtype=dtype, data=data, _already_exists=alrdy, **kwargs)
		except ValueError:
			return alrdy[0]

	def _save_data(self, f):
		name = self.name[1:]
		if name == '':
			return
		ent = Entry(pathname=name, size=0, mtime=time.time(), mode=stat.S_IFDIR)
		s = StringIO()
		np.save(s, [dict(self.attrs)])
		f.write(ent, data=s.getvalue())


class File(Group):
	def __init__(self, filename, mode):
		if not mode in ['r', 'r+', 'w', 'w-', 'a']:
			raise ValueError('Unsupported mode: %s' % mode)

		self.name = ''
		super(File, self).__init__(self, '', self) # The / group
		self._toc = {}

		self._filename = filename
		self._mode = mode

		if (mode == 'r' or mode == 'r+') and not os.path.exists(filename):
			raise IOError('File doesn\'t exist: %s' % filename)
		elif mode == 'w-' and os.path.exists(filename):
			raise IOError('File already exists: %s' % filename)
		elif not os.path.exists(filename):
			return

		f = Archive(filename, 'r')

		print 'Reading TOC...'
		for ent in f:
			print ent.pathname
			g = self
			frags = filter(lambda x: x != '', ent.pathname.split('/'))
			for frag in frags[:-1]:
				print frag
				if not frag in g._toc:
					g._toc[frag] = Group(self, frag, g)
				g = g._toc[frag]

			if ent.issym():
				g._toc[frags[-1]] = SoftLink(self, ent.pathname)
			elif ent.isdir():
				g._toc[frags[-1]] = Group(self, frags[-1], g)
				buf = f.read(ent.size)
				print 'ent.size:', ent.size, 'buf:', buf
				s = StringIO(buf)
				dump = np.load(s)
				g._toc[frags[-1]].attrs.clear()
				for (k, v) in dump[0]:
					g._toc[frags[-1]].attrs[k] = v
			else:
				g._toc[frags[-1]] = Dataset(self, frags[-1], g, data=ent)

			g._toc[frags[-1]]._touched = False

		f.close()


	@property
	def filename(self):
		return self._filename

	@property
	def mode(self):
		return self._mode

	def close(self):
		self.flush()

	def flush(self):
		print 'File.flush()'
		if self._mode != 'r':
			f = Archive(self._filename, mode='w')

		Q = [self]
		while len(Q) > 0:
			g = Q.pop()
			print g
			if g._touched:
				if self._mode == 'r':
					raise Exception('Can\'t write changes. File opened in read-only mode.')
				print 'Saving', g.name
				g._save_data(f)
			if isinstance(g, Group):
				Q += g._toc.values()

		if self._mode != 'r':
			f.f.flush()
			os.fsync(f.f.fileno())
			f.close()


class Dataset(object):
	def __init__(self, file_, entry_name, parent, shape=None, dtype=None, data=None):
		self.compression = None
		self.compression_opts = None

		self._attrs = AttributeManager(self)
		self._entry_name = entry_name
		self._name = parent.name + ('/' if parent.name == '' or parent.name[-1] != '/' else '') + entry_name
		self._file = file_
		self._parent = parent
		self._touched = True
		if isinstance(data, Entry):
			self._data = data
		elif data is None:
			if shape is None:
				raise ValueError('data and shape can\'t be None at the same time')
			data = np.zeros(shape, dtype=dtype)
		else:
			self._data = np.array(data, dtype=dtype)
		if not shape is None:
			self._data = self._data.reshape(shape)

	@property
	def parent(self):
		return self._parent

	@property
	def file_(self):
		return self._file

	@property
	def entry_name(self):
		return self._entry_name

	@property
	def name(self):
		return self._name

	@property
	def attrs(self):
		return self._attrs

	@property
	def shape(self):
		self._load_data()
		return self._data.shape

	@property
	def dtype(self):
		self._load_data()
		return self._data.dtype

	@property
	def size(self):
		self._load_data()
		return self._data.size

	@property
	def maxshape(self):
		self._load_data()
		return self._data.shape

	@property
	def dims(self):
		self._load_data()
		return self._data.dims

	@property
	def chunks(self):
		return None

	@property
	def scaleoffset(self):
		return None

	@property
	def shuffle(self):
		return False

	@property
	def fletcher32(self):
		return False

	@property
	def fillvalue(self):
		return None

	def _load_data(self):
		if isinstance(self._data, Entry):
			f = Archive(self._file.filename, mode='r')
			for ent in f:
				if ent.header_position == self._data.header_position:
					break
			rs = f.readstream(self._data.size)
			dump = np.load(rs)
			self._attrs.clear()
			for (k, v) in dump[0]:
				self._attrs[k] = v
			self._data = dump[1]
			f.close()
			self._touched = False

	def _save_data(self, f):
		self._load_data()
		name = self.name[1:]

		s = StringIO()
		np.save(s, [dict(self._attrs), self._data])
		dump = s.getvalue()
		ent = Entry(pathname = name, size=len(dump), mtime=time.time(), mode=stat.S_IFREG)
		f.write(ent, data=dump)

	def __getitem__(self, item):
		self._load_data()
		return self._data[item]

	def __setitem__(self, key, value):
		self._load_data()
		self._data[key] = value

	def read_direct(self, array, source_sel=None, dest_sel=None):
		raise NotImplementedError()

	def astype(self, dtype):
		self._load_data()
		return self._data.astype(dtype)

	def resize(self, size, axis=None):
		self._load_data()
		self._data.resize(size, axis=axis)

	def len(self):
		self._load_data()
		return len(self._data)
