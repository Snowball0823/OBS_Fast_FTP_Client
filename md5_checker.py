import os,sys
import yaml
import glob
from progressbar import *
from multiprocessing import Process, Manager, Value
import multiprocessing
import hashlib

class Md5Checker(object):
    def __init__(self, raw_files, raw_md5_values, buffer_size=81920):
        self.raw_files = raw_files
        self.raw_md5_values = raw_md5_values
        self.buffer_size = buffer_size

    @staticmethod
    def _get_big_file_md5(file_name, buffer_size):
        md5_value=hashlib.md5()
        with open(file_name, "rb") as trans_f:
            while True:
                data_flow = trans_f.read(int(buffer_size))
                if not data_flow:
                    break
                md5_value.update(data_flow)
        trans_f.close()
        return md5_value

    def update_files(self, history_file_path=''):
        nochanged_files, nochanged_md5 = [], []
        f = open(history_file_path, 'w+')
        for index, tmp_file in enumerate(self.raw_files):
            new_md5 = Md5Checker._get_big_file_md5(tmp_file, self.buffer_size)
            new_hex_value = new_md5.hexdigest()
            if self.raw_md5_values[index] == new_hex_value:
                nochanged_files.append(tmp_file)
                f.write(tmp_file+'\n')
                f.write(new_hex_value+'\n')
        f.close()
        return set(nochanged_files)

    def multi_process_update_files(self, history_file_path='', process_num=32):
        # to recreat the history file
        with open(history_file_path, 'w+') as f:
            f.close()
        manager = Manager()
        all_file_indx = Value('i', -1)
        files_count = Value('i', 0)
        all_file_list = manager.list(self.raw_files)
        all_md5_list = manager.list(self.raw_md5_values)
        nochanged_files = manager.list([])
        nochanged_md5 = manager.list([])
        weight = 1 if len(self.raw_files) > 100 else 10
        checkFiles_widgets = ['Checking History:',
                            Percentage(), Bar('☆'), Timer(), ' ', ETA()]
        checkFiles_bar = ProgressBar(
            widgets=checkFiles_widgets, maxval=weight * len(self.raw_files)).start()
        # start uploading
        all_process = [Process(target=self._sub_process_update_files, args=(history_file_path, checkFiles_bar, nochanged_files, weight, all_file_indx, all_file_list, all_md5_list, files_count))
                    for i in range(process_num)]
        for tmp_process in all_process:
            tmp_process.start()
        for tmp_process in all_process:
            tmp_process.join()
        checkFiles_bar.finish()
        return set(nochanged_files)

    def _sub_process_update_files(self, history_file_path, bar, nochanged_files, weight, file_indx, public_file_list, md5_list, files_count):
        while True:
            with file_indx.get_lock():
                if file_indx.value < len(public_file_list)-1:
                    file_indx.value += 1
                    tmp_file_index = file_indx.value
                else:
                    break
            file_name = public_file_list[tmp_file_index]
            # print(remote_name)
            file_md5 = Md5Checker._get_big_file_md5(file_name, self.buffer_size)
            with files_count.get_lock():
                files_count.value += 1
                count = files_count.value
                if md5_list[tmp_file_index] == file_md5.hexdigest():
                    nochanged_files.append(file_name)
                    with open(history_file_path, 'a+') as f:
                        f.write(file_name+'\n')
                        f.write(file_md5.hexdigest()+'\n')
                    f.close()
            bar.update(weight*count)