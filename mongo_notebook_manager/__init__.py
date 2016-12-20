"""A notebook manager for IPython with MongoDB as the backend."""

from tornado import web
from getpass import getuser
import os

from io import StringIO
import datetime

import pymongo

try:
    from mongodb_proxy import MongoProxy
except ImportError:
    from .mongodb_proxy import MongoProxy

from .utils.ipycompat import Unicode, CBool, ContentsManager, reads, new_notebook, to_notebook_json, writes


def sort_key(item):
    """Case-insensitive sorting."""
    return item['path'].lower()

#-----------------------------------------------------------------------------
# Classes
#-----------------------------------------------------------------------------


class MongoNotebookManager(ContentsManager):
    #Useless variable that is required unfortunately
    notebook_dir = Unicode(u"", config=True)

    mongo_uri = Unicode('mongodb://localhost:27017/', config=True,
        help="The URI to connect to the MongoDB instance. Defaults to 'mongodb://localhost:27017/'"
    )

    replica_set = Unicode('', config=True,
        help="Replica set for mongodb, if any"
    )

    database_name = Unicode('ipython', config=True,
        help="Defines the database in mongodb in which to store the collections"
    )

    notebook_collection = Unicode('notebooks', config=True,
        help="Defines the collection in mongodb in which to store the notebooks"
    )

    checkpoint_collection = Unicode('checkpoints', config=True,
        help="The collection name in which to keep notebook checkpoints"
    )

    checkpoints_history = CBool('checkpoints_history', config=True,
        help="Save all checkpoints or keep only last"
    )

    def __init__(self, **kwargs):
        super(MongoNotebookManager, self).__init__(**kwargs)
        if len(self.replica_set) == 0:
            self._conn = self._connect_server()
        else:
            self._conn = self._connect_replica_set()
        self.user_id = getuser()
        self.ensure_root_directory()

    def ensure_root_directory(self):
        model = {}
        model['name'] = ''
        model['path'] = ''
        model['last_modified'] = datetime.datetime.utcnow()
        model['user_id'] = self.user_id
        model['created'] = datetime.datetime.utcnow()
        model['writable'] = True
        model['mimetype'] = 'directory'
        model['format'] = None
        model['type'] = 'directory'
        model['content'] = None

        self.ensure_directory(model)

    def get_notebook_names(self, path=''):
        """List all notebook names in the notebook dir and path."""
        path = path
        spec = {'path': path,
                'type': 'notebook'}
        fields = {'path': 1}
        notebooks = list(self._connect_collection(self.notebook_collection).find(spec, fields))
        paths = [n['path'] for n in notebooks]
        return paths

    def path_exists(self, path):
        """Does the API-style path (directory) actually exist?

        Parameters
        ----------
        path : string
            The path to check. This is an API path (`/` separated,
            relative to base notebook-dir).

        Returns
        -------
        exists : bool
            Whether the path is indeed a directory.
        """

        spec = {'path': path}
        count = self._connect_collection(self.notebook_collection).find(spec).count()
        return count > 0

    def is_hidden(self, path):
        #Nothing is hidden
        return False

    def notebook_exists(self, path=''):
        spec = {
            'path': path,
            'type': 'notebook'
        }

        count = self._connect_collection(self.notebook_collection).find(spec).count()
        return count == 1

    def list_dirs(self, path):
        spec = {
            'path': path,
            'type': 'directory'
        }
        fields = {'path': 1}
        paths = list(self._connect_collection(self.notebook_collection).find(spec, fields))

        dirs = [self.get_dir_model(p['path']) for p in paths]
        dirs = sorted(dirs, key=sort_key)
        return dirs

    def get_dir_model(self, path):
        spec = {
            'path': path,
            'name': path,
            'type': 'directory'
        }
        fields = {
            'lastModified': 1,
            'created': 1,
            'user_id': 1
        }

        notebook = self._connect_collection(self.notebook_collection).find_one(spec, fields)
        if notebook is None:
            raise IOError('directory does not exist: %r' % path)

        last_modified = notebook['lastModified']
        created = notebook['created']
        # Create the notebook model.
        model = {}
        model['name'] = path
        model['path'] = path
        model['last_modified'] = last_modified
        model['user_id'] = notebook['user_id']
        model['created'] = created
        model['writable'] = False
        model['mimetype'] = 'directory'
        model['format'] = 'directory'
        model['type'] = 'directory'
        return model

    def get_dir(self, path='', content=True):
        model = self.get_dir_model(path)
        if content:
            model['content'] = self.list_notebooks(path) + self.list_dirs(path)
        return model

    def list_notebooks(self, path):
        notebook_names = self.get_notebook_names(path)
        notebooks = [self.get_notebook(path, content=True)
                     for path in notebook_names if self.should_list(path)]
        notebooks = sorted(notebooks, key=sort_key)
        return notebooks

    def get_notebook(self, path='', content=True):
        if not self.notebook_exists(path=path):
            raise web.HTTPError(404, u'Notebook does not exist: %s' % path)

        spec = {
            'path': path,
            'type': 'notebook'
        }
        fields = {
            'lastModified': 1,
            'created': 1,
            'user_id': 1
        }
        if content:
            fields['content'] = 1

        notebook = self._connect_collection(self.notebook_collection).find_one(spec, fields)

        last_modified = notebook['lastModified']
        created = notebook['created']
        # Create the notebook model.
        model = {}
        model['name'] = path
        model['path'] = path
        model['last_modified'] = last_modified
        model['created'] = created
        model['user_id'] = notebook['user_id']
        model['type'] = 'notebook'
        model['writable'] = True
        model['mimetype'] = 'notebook'
        model['format'] = 'json'
        if content:
            with StringIO(notebook['content']) as f:
                nb = reads(f, u'json')
            self.mark_trusted_cells(nb, path)
            model['content'] = nb
        return model

    def create_notebook(self, model=None, path=''):
        """Create a new notebook and return its model with no content."""
        if model is None:
            model = {}
        if 'content' not in model:
            model['content'] = new_notebook()

        model['path'] = path
        model['type'] = 'notebook'
        model = self.save_notebook(model, model['path'])

        return model

    def ensure_directory(self, model, path=''):
        """Create a new directory."""
        dir_model = dict()
        dir_model['name'] = model.get('name', path)
        dir_model['path'] = model.get('path', path)
        dir_model['type'] = model.get('type', 'directory')
        dir_model['last_modified'] = model.get('last_modified', datetime.datetime.utcnow())
        dir_model['created'] = model.get('created', datetime.datetime.utcnow())
        dir_model['content'] = model.get('content', None)
        dir_model['writable'] = model.get('writable', False)
        dir_model['format'] = model.get('format', None)
        dir_model['mimetype'] = model.get('mimetype', 'directory')

        if not self.path_exists(path):
            spec = {
                'name': dir_model['name'],
                'path': dir_model['path'],
            }
            data = {
                '$set': {
                    'type': dir_model['type'],
                    'lastModified': dir_model['last_modified'],
                    'user_id': self.user_id,
                    'created': dir_model['created'],
                    'content': dir_model['content'],
                    'writable': dir_model['writable'],
                    'format': dir_model['format'],
                    'mimetype': dir_model['mimetype']
                }
            }
            self._connect_collection(self.notebook_collection).update(spec, data, upsert=True)
        return dir_model

    def save_dir(self, model, path='', type='', ext=''):
        return self.ensure_directory(model, path)

    def save_notebook(self, model, path='', type='', ext=''):
        if 'content' not in model:
            raise web.HTTPError(400, u'No notebook JSON data provided')

        # One checkpoint should always exist
        if self.notebook_exists(path) and not self.list_checkpoints(path):
            self.create_checkpoint(path)

        new_path = model.get('path', path)
        new_name = model.get('name', path)

        if path != new_path:
            self.rename_notebook(path, new_path)

        # Save the notebook file
        nb = to_notebook_json(model['content'])

        self.check_and_sign(nb, new_path)

        try:
            with StringIO() as f:
                writes(nb, f, u'json')
                spec = {
                    'path': new_path,
                    'name': new_name,
                }
                data = {
                    '$set': {
                        'type': 'notebook',
                        'content': f.getvalue(),
                        'lastModified': datetime.datetime.now(),
                        'user_id': self.user_id
                    }
                }
                f.close()
                if 'created' in model:
                    data['$set']['created'] = model['created']
                else:
                    data['$set']['created'] = datetime.datetime.now()
                self._connect_collection(self.notebook_collection).update(spec, data, upsert=True)
        except Exception as e:
            raise web.HTTPError(400, u'Unexpected error while autosaving notebook: %s' % (e))
        model = self.get_notebook(new_path, content=False)

        return model

    def update_notebook(self, model, path=''):
        new_path = model.get('path', path)
        if path != new_path:
            self.rename_notebook(path, new_path)
        model = self.get_notebook(new_path, content=False)
        return model

    def delete_notebook(self, path=''):
        spec = {
            'path': path,
        }
        fields = {
            'path': 1,
        }

        notebook = self._connect_collection(self.notebook_collection).find_one(spec, fields)
        if not notebook:
            raise web.HTTPError(404, u'Notebook does not exist: %s' % path)

        # clear checkpoints
        self._connect_collection(self.checkpoint_collection).remove(spec)
        self._connect_collection(self.notebook_collection).remove(spec)

    def rename_notebook(self, old_path, new_path):
        old_path = old_path
        new_path = new_path
        if new_path == old_path:
            return

        # Should we proceed with the move?
        spec = {
            'path': new_path,
        }
        fields = {
            'path': 1,
        }
        notebook = self._connect_collection(self.notebook_collection).find_one(spec, fields)
        if notebook != None:
            raise web.HTTPError(409, u'Notebook with name already exists: %s' % new_path)

        # Move the notebook file
        try:
            spec = {
                'path': old_path
            }
            modify = {
                '$set': {
                    'path': new_path
                }
            }
            self._connect_collection(self.notebook_collection).update(spec, modify)
        except Exception as e:
            raise web.HTTPError(500, u'Unknown error renaming notebook: %s %s' % (old_path, e))

        # Move the checkpoints
        spec = {
            'path': old_path
        }
        modify = {
            '$set': {
                'path': new_path
            }
        }
        self._connect_collection(self.checkpoint_collection).update(spec, modify, multi=True)

    # public checkpoint API
    def create_checkpoint(self, path=''):
        path = path
        spec = {
            'path': path
        }

        notebook = self._connect_collection(self.notebook_collection).find_one(spec)
        chid = notebook['_id']
        del notebook['_id']
        cp_id = str(self._connect_collection(self.checkpoint_collection).find(spec).count())

        if self.checkpoints_history:
            spec['cp'] = cp_id
        else:
            notebook['cp'] = cp_id
            spec['id'] = chid

        newnotebook = {'$set': notebook}

        last_modified = notebook["lastModified"]
        self._connect_collection(self.checkpoint_collection).update(spec, newnotebook, upsert=True)

        # return the checkpoint info
        return dict(id=cp_id, last_modified=last_modified)

    def list_checkpoints(self, path=''):
        path = path
        spec = {
            'path': path
        }
        checkpoints = list(self._connect_collection(self.checkpoint_collection).find(spec))
        return [dict(id=c['cp'], last_modified=c['lastModified']) for c in checkpoints]

    def restore_checkpoint(self, checkpoint_id, path=''):
        path = path
        spec = {
            'path': path,
            'cp': checkpoint_id
        }

        checkpoint = self._connect_collection(self.checkpoint_collection).find_one(spec)

        if checkpoint == None:
            raise web.HTTPError(
                404, u'Notebook checkpoint does not exist: %s-%s' % (path, checkpoint_id)
            )
        del spec['cp']
        del checkpoint['cp']
        del checkpoint['_id']
        checkpoint = {'$set': checkpoint}
        self._connect_collection(self.notebook_collection).update(spec, checkpoint, upsert=True)

    def delete_checkpoint(self, checkpoint_id, path=''):
        path = path
        spec = {
            'path': path,
            'cp': checkpoint_id
        }
        checkpoint = self._connect_collection(self.checkpoint_collection).find_one(spec)
        if checkpoint == None:
            raise web.HTTPError(404,
                u'Notebook checkpoint does not exist: %s-%s' % (path, checkpoint_id)
            )
        self._connect_collection(self.checkpoint_collection).remove(spec)

    def info_string(self):
        return "Serving notebooks from mongodb"

    def get_kernel_path(self, path='', model=None):
        return os.path.join(self.notebook_dir, path)

    #mongodb related functions
    def _connect_server(self):
        return MongoProxy(pymongo.MongoClient(self.mongo_uri))

    def _connect_replica_set(self):
        return MongoProxy(pymongo.MongoReplicaSetClient(self.mongo_uri, self._replicaSet))

    def _connect_collection(self, collection):
        if not self._conn:
            if len(self.replica_set) == 0:
                self._conn = self._connect_server()
            else:
                self._conn = self._connectReplicaSet()
        return self._conn[self.database_name][collection]

    def get(self, path, content=True, type=None, format=None, **kwargs):
        model_type = type or self.guess_type(path)
        try:
            fn = {
                'notebook': self.get_notebook,
                'directory': self.get_dir,
                'file': self.get_notebook,
            }[model_type]
        except KeyError:
            raise ValueError("Unknown type passed: '{}'".format(model_type))
        try:
            return fn(path, content=content)
        except Exception as e:
            raise web.HTTPError(500,
                u'Error at path {}. {}'.format(path, e)
            )

    def save(self, model, path):
        model_type = model.get('type') or self.guess_type(path)
        try:
            fn = {
                'notebook': self.save_notebook,
                'directory': self.save_dir,
                'file': self.save_notebook,
            }[model['type']]
        except KeyError:
            raise ValueError("Unknown type passed: '{}'".format(model.get('type')))
        try:
            return fn(model, path, model_type)
        except Exception as e:
            raise web.HTTPError(500,
                u'Error at path {}. {}'.format(path, e)
            )

    def delete_file(self, path):
        return self.delete_notebook(path)

    def rename_file(self, old_path, new_path):
        return self.rename_notebook(old_path, new_path)

    def file_exists(self, path):
        return self.notebook_exists(path)

    def dir_exists(self, path):
        return self.path_exists(path)

    def guess_type(self, path, allow_directory=True):
        """
        Guess the type of a file.
        If allow_directory is False, don't consider the possibility that the
        file is a directory.
        """
        if path.endswith('.ipynb'):
            return 'notebook'
        elif self.dir_exists(path):
            return 'directory'
        else:
            return 'file'
