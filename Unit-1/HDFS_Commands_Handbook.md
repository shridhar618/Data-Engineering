# HDFS Commands Handbook

## 1. Check HDFS

``` bash
hdfs dfs -ls /
```

Lists files/directories in HDFS root.

## 2. Create Directory

``` bash
hdfs dfs -mkdir /data
hdfs dfs -mkdir -p /data/input/logs
```

## 3. List Directory

``` bash
hdfs dfs -ls /data
hdfs dfs -ls -R /data
```

## 4. Upload Files

``` bash
hdfs dfs -put file.txt /data
hdfs dfs -copyFromLocal file.txt /data
```

## 5. Download Files

``` bash
hdfs dfs -get /data/file.txt .
hdfs dfs -copyToLocal /data/file.txt .
```

## 6. View File

``` bash
hdfs dfs -cat /data/file.txt
hdfs dfs -tail /data/file.txt
hdfs dfs -text /data/file.gz
```

## 7. Copy Files

``` bash
hdfs dfs -cp /data/file1 /backup/file1
```

## 8. Move/Rename

``` bash
hdfs dfs -mv /data/file1 /archive/file1
```

## 9. Delete

``` bash
hdfs dfs -rm /data/file.txt
hdfs dfs -rm -r /data
hdfs dfs -rmdir /emptydir
hdfs dfs -expunge
```

## 10. Append

``` bash
hdfs dfs -appendToFile local.txt /data/file.txt
```

## 11. File Size

``` bash
hdfs dfs -du /data
hdfs dfs -du -h /data
hdfs dfs -count /data
```

## 12. Disk Usage

``` bash
hdfs dfs -df -h
```

## 13. Permissions

``` bash
hdfs dfs -chmod 755 /data
hdfs dfs -chown hadoop:hadoop /data
hdfs dfs -chgrp admin /data
```

## 14. Replication

``` bash
hdfs dfs -setrep 3 /data/file.txt
```

## 15. Checksum

``` bash
hdfs dfs -checksum /data/file.txt
```

## 16. Touch

``` bash
hdfs dfs -touchz /data/empty.txt
```

## 17. Find

``` bash
hdfs dfs -find /data -name "*.csv"
```

## 18. Merge Files

``` bash
hdfs dfs -getmerge /logs merged.txt
```

## 19. Test Commands

``` bash
hdfs dfs -test -e /data/file.txt
hdfs dfs -test -d /data
hdfs dfs -test -z /data/empty.txt
```

## 20. Statistics

``` bash
hdfs dfs -stat "%b %n %o" /data/file.txt
```

## 21. fsck

``` bash
hdfs fsck /
hdfs fsck /data -files -blocks -locations
```

## 22. Safe Mode

``` bash
hdfs dfsadmin -safemode enter
hdfs dfsadmin -safemode leave
hdfs dfsadmin -safemode get
```

## 23. Report

``` bash
hdfs dfsadmin -report
```

## 24. Refresh Nodes

``` bash
hdfs dfsadmin -refreshNodes
```

## 25. Balancer

``` bash
hdfs balancer
```

## 26. Quotas

``` bash
hdfs dfsadmin -setQuota 100 /data
hdfs dfsadmin -clrQuota /data
hdfs dfsadmin -setSpaceQuota 10g /data
```

## 27. Snapshots

``` bash
hdfs dfsadmin -allowSnapshot /data
hdfs dfs -createSnapshot /data snap1
hdfs dfs -deleteSnapshot /data snap1
```

## 28. Trash

``` bash
hdfs dfs -rm /data/file.txt
hdfs dfs -expunge
```

## 29. Common Interview Commands

-   `hdfs dfs -ls`
-   `hdfs dfs -mkdir`
-   `hdfs dfs -put`
-   `hdfs dfs -get`
-   `hdfs dfs -cp`
-   `hdfs dfs -mv`
-   `hdfs dfs -rm`
-   `hdfs dfs -cat`
-   `hdfs dfs -du`
-   `hdfs dfs -count`
-   `hdfs dfs -chmod`
-   `hdfs dfs -chown`
-   `hdfs dfs -setrep`
-   `hdfs fsck`
-   `hdfs dfsadmin -report`
-   `hdfs dfsadmin -safemode`

## Frequently Asked Viva Questions

1.  Difference between `-put` and `-copyFromLocal`
2.  Difference between `-get` and `-copyToLocal`
3.  Difference between `-rm -r` and `-rmdir`
4.  What is replication factor?
5.  What is Safe Mode?
6.  What is fsck?
7.  What is NameNode?
8.  What is DataNode?
9.  What is block size?
10. What is checkpointing?
