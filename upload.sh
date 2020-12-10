echo 'Starting server...'
_=`python2 ./OBSFTP/FTPServerStart.py > ./ftp_sever.log 2>&1 &`
sleep 2
python3 ftp_upload.py
kill -9 `ps -ef|grep 'python2 ./OBSFTP/FTPServerStart.py'|grep -v grep|awk '{print $2}'`