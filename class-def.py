import argparse
import dropbox
import six
import sys
import os
import time
import datetime
import unicodedata

class helpermethods:
    #Define method to list the directories.
    def list_folder(self,dbx, folder, subfolder):
        """List a folder.
        Return a dict mapping unicode filenames to
        FileMetadata|FolderMetadata entries.
        """
        path = '/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'))
        while '//' in path:
            path = path.replace('//', '/')
        path = path.rstrip('/')
        try:
            with stopwatch('list_folder'):
                res = dbx.files_list_folder(path)
        except dropbox.exceptions.ApiError as err:
            print('Folder listing failed for', path, '-- assumed empty:', err)
            return {}
        else:
            rv = {}
            for entry in res.entries:
                rv[entry.name] = entry
            return rv