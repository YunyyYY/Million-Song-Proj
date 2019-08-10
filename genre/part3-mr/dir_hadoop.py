
from utils import get_hdfs_h5_files
from hdfs3 import *  

hdfs = HDFileSystem(host='localhost', port=9000)
paths = get_hdfs_h5_files("/A/", hdfs)

with open('dir.txt', 'w') as f:
    for i in paths:
        f.write("hdfs://localhost:9000"+i[0:-1]+"\n")

hdfs.put('dir.txt', "/dir.txt")