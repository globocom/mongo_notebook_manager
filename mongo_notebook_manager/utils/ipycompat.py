"""
Utilities for managing IPython 3/4 compat.
"""
import IPython

SUPPORTED_VERSIONS = {3, 4, 5}
IPY_MAJOR = IPython.version_info[0]
if IPY_MAJOR not in SUPPORTED_VERSIONS:
    raise ImportError("IPython version %d is not supported." % IPY_MAJOR)

IPY3 = (IPY_MAJOR == 3)

if IPY3:
    from IPython.config import Config
    from IPython.html.services.contents.manager import ContentsManager
    from IPython.html.services.contents.checkpoints import (
        Checkpoints,
        GenericCheckpointsMixin,
    )
    from IPython.html.services.contents.filemanager import FileContentsManager
    from IPython.html.services.contents.filecheckpoints import (
        GenericFileCheckpoints
    )
    from IPython.html.utils import to_os_path
    from IPython.nbformat import from_dict, reads, writes
    from IPython.nbformat.v4.nbjson import to_notebook as to_notebook_json
    from IPython.nbformat.v4.nbbase import (
        new_code_cell,
        new_markdown_cell,
        new_notebook,
        new_raw_cell,
    )
    from IPython.nbformat.v4.rwbase import strip_transient
    from IPython.utils.traitlets import (
        Any,
        Bool,
        CBool,
        Dict,
        Instance,
        Integer,
        HasTraits,
        Unicode,
    )
else:
    from traitlets.config import Config
    from notebook.services.contents.checkpoints import (
        Checkpoints,
        GenericCheckpointsMixin,
    )
    from notebook.services.contents.filemanager import FileContentsManager
    from notebook.services.contents.filecheckpoints import (
        GenericFileCheckpoints
    )
    from notebook.services.contents.manager import ContentsManager
    from notebook.utils import to_os_path
    from nbformat import from_dict, reads, writes
    from nbformat.v4.nbjson import to_notebook as to_notebook_json
    from nbformat.v4.nbbase import (
        new_code_cell,
        new_markdown_cell,
        new_notebook,
        new_raw_cell,
    )
    from nbformat.v4.rwbase import strip_transient
    from traitlets import (
        Any,
        Bool,
        CBool,
        Dict,
        Instance,
        Integer,
        HasTraits,
        Unicode,
    )

__all__ = [
    'Any',
    'Bool',
    'CBool',
    'Checkpoints',
    'Config',
    'ContentsManager',
    'Dict',
    'FileContentsManager',
    'GenericCheckpointsMixin',
    'GenericFileCheckpoints',
    'HasTraits',
    'Instance',
    'Integer',
    'Unicode',
    'from_dict',
    'new_code_cell',
    'new_markdown_cell',
    'new_notebook',
    'new_raw_cell',
    'reads',
    'to_notebook_json',
    'strip_transient',
    'to_os_path',
    'writes',
]
