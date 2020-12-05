> # Intro
OBS Fast FTP Client is a special FTP client for OBS FTP, which has uploading, downloading and other actions for remote HuaWei cloud server.

OBS Server Usage: [OBS Server](https://github.com/huaweicloud-obs/obsftp)
***
> ## Description
TO BE
> # Useage
Linux/Mac： Open terminal
``` shell
$ screen -S Your_Name
$ python2 FTPServerStart.py
Ctrl+A+D
$ python3 ftp_upload.py
``` 
> # Modify YAML
+ ip_address: 127.0.0.1 (default)
+ port: 10020 (default)
+ access_key_id: get from HUAWEI cloud platform
+ access_key_secret: get from HUAWEI cloud platform
+ bucket_name: your bucket name
+ remote_folder: default '' means the name is your upload folder name
+ action: start/resume/update. The first time uploading the files, you can use "start" action, if you add some files or the net work is down, then you can use "resume" action to upload the rest files. But if you have changed some files, you can use "update" action to update the files on cloud servers, the uploader can use MD5 checker to find the changed file and only upload the changed files and the added files to the cloud servers.
> # Ignore File
Like the git ignore regulation, the "[.ftpignore](./.ftpignore)" is an template file to show how to write an ignore file.
The ignore file is supposed to be biult beneath the folder you want to upload, unless you just want to upload a file.

