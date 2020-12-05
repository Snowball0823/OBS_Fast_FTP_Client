import os,sys
import yaml
import glob
from multiprocessing import Process, Manager, Value
import multiprocessing
from md5_checker import Md5Checker

class FileFilter(object):
    def __init__(self, local_path, history_name='.ftphistory', ignore_name='.ftpignore', buffer_size=81920):
        # files_set = make_files_set('') if os.path.isdir(local_path) else {os.path.split(local_path)[-1]}
        # files_list = list(files_set - set(history))
        self.local_path = local_path
        self.files_set = set()
        self.ignore_file_name = ignore_name
        self.history_file_name = history_name
        self.buffer_size = buffer_size

    def _get_file_set_without_ignore(self, files_set, folder):
        current_level_files = set(glob.glob(os.path.join(folder, '*')))
        for tmp_file in current_level_files:
            if os.path.isdir(tmp_file):
                self._get_file_set_without_ignore(files_set, tmp_file)
            else:
                files_set.add(tmp_file)

    def _get_file_set_with_ignore(self, files_set, folder, ignore_file, ignore_folder):
        current_ignore_file = set()
        current_level_files = set(glob.glob(os.path.join(folder, '*')))
        _ = [current_ignore_file.update(set(glob.glob(os.path.join(folder, i)))) for i in ignore_file]
        current_level_files = current_level_files - current_ignore_file
        for tmp_file in current_level_files:
            if os.path.isdir(tmp_file):
                folder_name = os.path.split(tmp_file)[-1]
                if folder_name not in ignore_folder:
                    self._get_file_set_with_ignore(files_set, tmp_file, ignore_file, ignore_folder)
            else:
                files_set.add(tmp_file)

    def filter(self, his_files, his_files_md5, md5_check=False, process_num=32):
        self.his_files = his_files
        self.his_files_md5 = his_files_md5
        if not os.path.isdir(self.local_path):
            self.files_set = {os.path.split(self.local_path)[-1]}
        else:
            if self.ignore_file_name not in os.listdir(self.local_path):
                print('We recommand to build a \''+self.ignore_file_name+'\' file!')
                print('It is easy to build a file, \''+self.ignore_file_name+'\', to filter the files you want to upload.')
                self._get_file_set_without_ignore(self.files_set, '')
            else:
                ignore_file = open(os.path.join(self.local_path, self.ignore_file_name), 'r') 
                load_func = yaml.full_load if yaml.__version__ >= '5.1' else yaml.load
                ignore_dict = load_func(ignore_file)
                self.ignore_file = ignore_dict['File']
                self.ignore_file += [self.history_file_name, self.ignore_file_name]
                self.ignore_file = list(set(self.ignore_file))
                self.ignore_folder = ignore_dict['Folder']
                self._get_file_set_with_ignore(self.files_set, '', self.ignore_file, self.ignore_folder)
        if not md5_check:
            files_list = list(self.files_set - set(self.his_files))
        else:
            history_file_path = os.path.join(self.local_path, self.history_file_name)
            raw_files_set = self.files_set - set(self.his_files)
            self.md5_checker = Md5Checker(self.his_files, self.his_files_md5, buffer_size=self.buffer_size)
            # update_history = self.md5_checker.update_files(history_file_path=history_file_path)
            update_history = self.md5_checker.multi_process_update_files(history_file_path=history_file_path, process_num=process_num)
            changed_file = set(self.his_files) - update_history
            print('*'*5+'Changed Files'+'*'*5)
            for i in changed_file:
                print(i)
            print('*'*5+'-------------'+'*'*5)
            self.his_files = update_history
            files_list = list(raw_files_set | changed_file)
        return files_list