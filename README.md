# Smart-Hadoop-Cluster-SMHACL

This is an automated hadoop cluster building tool.which implements distributed computing for creating the cluster over the network. This is implemented in python 2.7

Smart Hadoop Cluster, is a tool for building hadoop cluster over the network, using very less amount of time.
This tool uses the approach of distributed computing, for creating the cluster. SMHACL, is written in Python 2.7.

#### Author: [Ravin Kumar](https://mr-ravin.github.io)

#### Directory Architecture of SMHACL:
```
    SMHACL/
      |
      |--SMHACL.py
      |--servr.py
      |--client.py
      |--network_operation.py
      |--info.py
      |--softwares/        (it is a directory)
             |--hadoop1/   (it is a directory)            
             |--hadoop2/   (it is a directory)
             |--java/      (it is a directory)               
             |--packages/  (it is a directory)
                 
```

#### NOTE:  the computer on which this software will be run, will became Client.

- hadoop1 contains the .rpm file of hadoop1.

- hadoop2 contains the .tar.gz file of hadoop2.

- java contains the jdk.

- packages contains- hive(tar.gz), dockers(.rpm), pig(tar.gz) and splunk (.rpm)


## Working Demonstration

[![Working Demonstration](https://github.com/mr-ravin/Smart-Hadoop-Cluster-SMHACL/blob/master/SMHACL.gif)](https://github.com/mr-ravin/Smart-Hadoop-Cluster-SMHACL/blob/master/SMHACL.gif)

[Certificate Provided from Linuxworld Informatics Private Limited.](https://github.com/mr-ravin/Smart-Hadoop-Cluster-SMHACL/blob/master/SMHACL-Legal-Document.pdf)  


#### Running the Software:-

- SMHACL.py is the starting program, which starts : 
- info.py, for  getting informations of other connected system, like- free memory.
- network_operation.py which provides ip list of all the remaining computers. except the one on which the script is running.
- servr.py, it is responsible for creation of client. and sending information to client.py, which creates respective systems on other computer.

#### Steps:-

- info.py is send  to all connected computers by SMHACL.py, to collect the memory regarding information.
- servr.py creates the client on the current system.
- client.py is send to all the other computers to make them that computer(i.e. name node, data node, job tracker, task tracker), as described by
the instructions send by servr.py

```python

Copyright (c) 2016 Ravin Kumar
Website: https://mr-ravin.github.io

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation 
files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, 
modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the 
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the 
Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE 
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, 
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
