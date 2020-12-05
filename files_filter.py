import os,sys
import yaml
import glob
from multiprocessing import Process, Manager, Value
import multiprocessing


class FileFilter(object):
    def __init__(self, local_path):
        # files_set = make_files_set('') if os.path.isdir(local_path) else {os.path.split(local_path)[-1]}
        # files_list = list(files_set - set(history))
        self.local_path = local_path
        self.files_set = set()
        self.ignore_file_name = '.ftpignore'

    def _get_file_set_without_ignore(self, files_set, floder):
        current_level_files = set(glob.glob(os.path.join(floder, '*')))
        for tmp_file in current_level_files:
            if os.path.isdir(tmp_file):
                self._get_file_set_without_ignore(files_set, tmp_file)
            else:
                files_set.add(tmp_file)

    def _get_file_set_with_ignore(self, files_set, floder, ignore_file, ignore_floder):
        current_ignore_file = set()
        current_level_files = set(glob.glob(os.path.join(floder, '*')))
        _ = [current_ignore_file.update(set(glob.glob(os.path.join(floder, i)))) for i in ignore_file]
        current_level_files = current_level_files - current_ignore_file
        for tmp_file in current_level_files:
            if os.path.isdir(tmp_file):
                floder_name = os.path.split(tmp_file)[-1]
                if floder_name not in ignore_floder:
                    self._get_file_set_with_ignore(files_set, tmp_file, ignore_file, ignore_floder)
            else:
                files_set.add(tmp_file)

    def filter(self, history):
        self.history = history
        if not os.path.isdir(self.local_path):
            self.files_set = {os.path.split(self.local_path)[-1]}
        else:
            if self.ignore_file_name not in os.listdir(self.local_path):
                print('We recommand to build a \'.ftpignore\' file!')
                print('It is easy to build a file, \'.ftpignore\', to filter the files you want to upload.')
                self._get_file_set_without_ignore(self.files_set, '')
            else:
                ignore_file = open(os.path.join(self.local_path, self.ignore_file_name), 'r') 
                load_func = yaml.full_load if yaml.__version__ >= '5.1' else yaml.load
                ignore_dict = load_func(ignore_file)
                self.ignore_file = ignore_dict['File']
                self.ignore_file += ['.ftpignore', '.ftphistory']
                self.ignore_file = list(set(self.ignore_file))
                self.ignore_floder = ignore_dict['Floder']
                self._get_file_set_with_ignore(self.files_set, '', self.ignore_file, self.ignore_floder)
        files_list = list(self.files_set - set(self.history))
        return files_list