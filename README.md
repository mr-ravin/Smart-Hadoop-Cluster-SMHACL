# Smart-Hadoop-Cluster-SMHACL

This is an automated hadoop cluster building tool.which implements distributed computing for creating the cluster over the network. This is implemented in python 2.7

Smart Hadoop Cluster, is a tool for building hadoop cluster over the network, using very less amount of time.
This tool uses the approach of distributed computing, for creating the cluster. SMHACL, is written in Python 2.7.
SMHACL is created, designed and developed by Mr. Ravin Kumar. New approaches are welcome to share, following are
his contact details : 
[Email:](mr.ravin_kumar@hotmail.com)  
[Linkedin:](https://in.linkedin.com/in/ravinkumar21)

This is the directory structure of SMHACL:

    SMHACL

      |--SMHACL.py
  
      |--servr.py
  
      |--client.py
  
      |--network_operation.py
  
      |--info.py
  
      |
  
      |--softwares
  
        |--------|--hadoop1   (it is a directory)
        
                 |--hadoop2   (it is a directory)
                 
                 |--java      (it is a directory)
                 
                 |--packages  (it is a directory)
                 
#### NOTE:  the computer on which this software will be run, will became Client.

- hadoop1 contains the .rpm file of hadoop1.

- hadoop2 contains the .tar.gz file of hadoop2.

- java contains the jdk.

- packages contains- hive(tar.gz), dockers(.rpm), pig(tar.gz) and splunk (.rpm)

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
