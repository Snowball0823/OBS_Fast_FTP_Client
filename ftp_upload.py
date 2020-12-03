import os, sys
import yaml
import glob
import atexit 
from ftplib import FTP
from progressbar import *
from multiprocessing import Process, Manager, Value
import multiprocessing

Config = "config.yml"

def _get_current_file_set(files_set, floder):
    current_level_files = set(glob.glob(os.path.join(floder, '*')))
    for tmp_file in current_level_files:
        if os.path.isdir(tmp_file):
            _get_current_file_set(files_set, tmp_file)
        else:
            files_set.add(tmp_file)


def _ftp_client_login(remote_floder='', user='', pwd='', ip='127.0.0.1', port='22', buffer_size=8192):
    sub_ftp = FTP()
    sub_ftp.connect(ip, port)
    sub_ftp.login(user, pwd)
    return sub_ftp, buffer_size, remote_floder


def make_files_set(floder=''):
    final_files_set = set()
    _get_current_file_set(final_files_set, floder)
    return final_files_set


def multi_ftp_client(f_path, bar, weight, file_index, public_file_list, files_count, **kwargs):
    sub_ftp, buffer_size, remote_floder = _ftp_client_login(**kwargs)
    while True:
        with file_index.get_lock():
            if file_index.value < len(public_file_list)-1:
                file_index.value += 1
                tmp_file_index = file_index.value
            else:
                break
        file_name = public_file_list[tmp_file_index]
        remote_name = remote_floder+'/'+file_name
        # print(remote_name)
        with open(file_name, "rb") as trans_f:
            try:
                sub_ftp.storbinary('STOR '+remote_name, trans_f, buffer_size)
            except sub_ftp.error_perm as err:
                raise AssertionError(str(err))
        with files_count.get_lock():
            files_count.value += 1
            count = files_count.value
            with open(f_path, 'a+') as f:
                f.write(file_name+'\n')
        bar.update(weight*count)
    sub_ftp.close()


def main(config):
    server_config = config['Server']
    local_config = config['Local']
    option = config['Option']
    ftp_usr_name = server_config['access_key_id'] + '/' + server_config['bucket_name']
    ftp_pwd = server_config['access_key_secret']
    ftp_ip = server_config['ip_address']
    ftp_port = server_config['port']
    local_path = local_config['upload_path']
    local_floder = local_path if os.path.isdir(local_path) else os.path.split(local_path)[0]
    execurate_file_path, _ = os.path.split(os.path.abspath( __file__))
    his_save_path = os.path.join(execurate_file_path, local_config['save_name'])
    _remote_floder = server_config['remote_floder']
    remote_floder = _remote_floder if _remote_floder != '' else os.path.join('/', os.path.split(local_floder)[-1])
    start = option['start']
    resume = option['resume']
    process_num = option['process_num']
    buffer_size = option['const_buffer_size']
    # build user info
    user_info = dict(
        user=ftp_usr_name,
        pwd=ftp_pwd,
        ip=ftp_ip,
        port=ftp_port,
        buffer_size=buffer_size,
        remote_floder=remote_floder
    )
    # build upload file list
    if resume:
        assert os.path.exists(his_save_path), 'Please check the hishtory file path!'
        print('-'*10+'Resume'+10*'-')
        print('Resume from '+his_save_path)
        print('Loading history now...')
        f = open(his_save_path, 'r')
        history = [i.strip('\n') for i in f.readlines()]
        f.close()
        print('-'*5+'Loading finish'+5*'-')
        assert history[0] == local_floder, 'History not match! Please use \"start\" option!'
        del(history[0])
    else:
        print('-'*10+'Start'+10*'-')
        print('Trans from '+local_floder)
        f = open(his_save_path, 'w+')
        f.write(local_floder+'\n')
        history = []
        f.close()
    old_work_path = os.getcwd()
    os.chdir(local_floder)
    print('Loading transport file names...')
    files_set = make_files_set('') if os.path.isdir(local_path) else {os.path.split(local_path)[-1]}
    files_list = list(files_set - set(history))
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
        # with open(his_save_path, 'a+') as his_f:
        all_process = [Process(target=multi_ftp_client, args=(his_save_path, uploadFiles_bar, weight, all_file_indx, all_file_list, files_count), kwargs=user_info)
                    for i in range(process_num)]
        for tmp_process in all_process:
            tmp_process.start()
        for tmp_process in all_process:
            tmp_process.join()
        uploadFiles_bar.finish()
        os.chdir(old_work_path)
    print(10*'-'+'Finshed'+'-'*10)




if __name__ == "__main__":
    root_path, _ = os.path.split(os.path.abspath( __file__))
    conf = open(os.path.join(root_path, Config), 'r')
    conf_dict = yaml.load(conf, Loader=yaml.FullLoader)
    conf.close()
    main(conf_dict)