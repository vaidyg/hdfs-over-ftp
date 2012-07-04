hdfs-over-ftp
=============
FTP server which works on a top of HDFS
Source code is provided under Apache License 2.0

FTP server is configurable by hdfs-over-ftp.properties and users.properties. It allows to use secure connection over SSL and supports all HDFS permissions.

Installation and running
1. Download and install java, maven
2. Set users in src/main/resources/users.properties. All passwords are md5 encrypted.
3. Set connection port, data-ports and hdfs-uri in src/main/resources/hdfs-over-ftp.properties.
4. Start server using hdfs-over-ftp.sh

Under linux you can mount ftp using curlftpfs:
sudo curlftpfs  -o allow_other ftp://user:pass@localhost:2222 ftpfs
