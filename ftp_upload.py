import os, sys
import yaml
import glob
import atexit
from ftplib import FTP
from progressbar import *
from multiprocessing import Process, Manager, Value
import multiprocessing
import hashlib 
from files_filter import FileFilter
from md5_checker import Md5Checker

Config = "config.yml"
History = ".ftphistory"
Ignore = ".ftpignore"

def _ftp_client_login(remote_folder='', user='', pwd='', ip='127.0.0.1', port='22', buffer_size=8192):
    sub_ftp = FTP()
    sub_ftp.connect(ip, port)
    sub_ftp.login(user, pwd)
    return sub_ftp, buffer_size, remote_folder

def multi_ftp_client(f_path, bar, weight, file_index, public_file_list, files_count, **kwargs):
    sub_ftp, buffer_size, remote_folder = _ftp_client_login(**kwargs)
    while True:
        with file_index.get_lock():
            if file_index.value < len(public_file_list)-1:
                file_index.value += 1
                tmp_file_index = file_index.value
            else:
                break
        file_name = public_file_list[tmp_file_index]
        remote_name = remote_folder+'/'+file_name
        # print(remote_name)
        with open(file_name, "rb") as trans_f:
            try:
                sub_ftp.storbinary('STOR '+remote_name, trans_f, buffer_size)
            except sub_ftp.error_perm as err:
                raise AssertionError(str(err))
        trans_f.close()
        file_md5 = Md5Checker._get_big_file_md5(file_name, int(buffer_size))
        with files_count.get_lock():
            files_count.value += 1
            count = files_count.value
            with open(f_path, 'a+') as f:
                f.write(file_name+'\n')
                f.write(file_md5.hexdigest()+'\n')
        bar.update(weight*count)
    sub_ftp.close()


def main(config):
    server_config = config['Server']
    local_config = config['Local']
    history_name = config['history_name']
    ignore_name = config['ignore_name']
    option = config['Option']
    ftp_usr_name = server_config['access_key_id'] + '/' + server_config['bucket_name']
    ftp_pwd = server_config['access_key_secret']
    ftp_ip = server_config['ip_address']
    ftp_port = server_config['port']
    local_path = local_config['upload_path']
    local_folder = local_path if os.path.isdir(local_path) else os.path.split(local_path)[0]
    execurate_file_path, _ = os.path.split(os.path.abspath( __file__))
    his_save_path = os.path.join(local_folder, history_name)
    _remote_folder = server_config['remote_folder']
    remote_folder = _remote_folder if _remote_folder != '' else os.path.join('/', os.path.split(local_folder)[-1])
    action = option['action']
    process_num = option['process_num']
    buffer_size = option['const_buffer_size']
    # build user info
    user_info = dict(
        user=ftp_usr_name,
        pwd=ftp_pwd,
        ip=ftp_ip,
        port=ftp_port,
        buffer_size=buffer_size,
        remote_folder=remote_folder
    )
    # change work path
    old_work_path = os.getcwd()
    os.chdir(local_folder)
    # build upload file list
    assert action in ['resume', 'start', 'update'], 'Please check the action in config file! The action: '+action+' can not be recognized!'
    if action == 'resume':
        md5_check = False
        assert os.path.exists(his_save_path), 'Please check the hishtory file path! Or just use \'start\' option!'
        print('-'*10+'Resume'+10*'-')
        print('Resume from '+his_save_path)
        print('Loading history now...')
        f = open(his_save_path, 'r')
        history_md5 = [i.strip('\n') for i in f.readlines()]
        history = history_md5[::2]
        files_md5 = history_md5[1::2]
        f.close()
        print('-'*5+'Loading finish'+5*'-')
    elif action == 'start':
        md5_check = False
        print('-'*10+'Start'+10*'-')
        print('Trans from '+local_folder)
        f = open(his_save_path, 'w+')
        history = []
        files_md5 = []
        f.close()
    elif action == 'update':
        print('-'*10+'Update'+10*'-')
        print('Cehcking '+local_folder)
        if os.path.exists(his_save_path):
            md5_check = True
            print('Have history, loading history now...')
            f = open(his_save_path, 'r')
            history_md5 = [i.strip('\n') for i in f.readlines()]
            history = history_md5[::2]
            files_md5 = history_md5[1::2]
            f.close()
        else:
            md5_check = False
            print('Have no history, updating the whole floder...')
            f = open(his_save_path, 'w+')
            history = []
            files_md5 = []
            f.close()
    print('Loading transport file names...')
    files_filter = FileFilter(local_path, history_name=history_name, ignore_name=ignore_name, buffer_size=int(buffer_size))
    files_list = files_filter.filter(history, files_md5, md5_check=md5_check, process_num=process_num)
    if len(files_list) == 0:
        print('All files are uploaded!')
    else:
        # set public memory
        manager = Manager()
        all_file_indx = Value('i', -1)
        files_count = Value('i', 0)
        all_file_list = manager.list(files_list)
        # set progress bar
        weight = 1 if len(files_list) > 100 else 10
        uploadFiles_widgets = ['Uploading Files:',
                            Percentage(), Bar('☆'), Timer(), ' ', ETA()]
        uploadFiles_bar = ProgressBar(
            widgets=uploadFiles_widgets, maxval=weight * len(files_list)).start()
        # start uploading
        all_process = [Process(target=multi_ftp_client, args=(his_save_path, uploadFiles_bar, weight, all_file_indx, all_file_list, files_count), kwargs=user_info)
                    for i in range(process_num)]
        for tmp_process in all_process:
            tmp_process.start()
        for tmp_process in all_process:
            tmp_process.join()
        uploadFiles_bar.finish()
    # change back
    os.chdir(old_work_path)
    print(10*'-'+'Finshed'+'-'*10)




if __name__ == "__main__":
    root_path, _ = os.path.split(os.path.abspath( __file__))
    conf = open(os.path.join(root_path, Config), 'r')
    load_func = yaml.full_load if yaml.__version__ >= '5.1' else yaml.load
    conf_dict = load_func(conf)
    conf_dict.update({'history_name':History, 'ignore_name':Ignore})
    conf.close()
    main(conf_dict)