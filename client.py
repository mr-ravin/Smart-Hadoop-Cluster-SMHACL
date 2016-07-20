import os,sys,socket,commands

def gen_coresite(name_node_ip,port=10001):
  #A function for developing core-site.xml
  #Parameter name_node_ip holds the ip address of name node.
  #Parameter port defines the port number for the service.
  #Parameter send_ip_addr holds the ip address of the system where the file have to be send. 
  #Parameter password holds the root account password of the send_ip_addr system.
 
  core_read=open("/etc/hadoop/core-site.xml","r")
  # data_core_site is a string containing all the data of core-site.xml
  data_core_site= core_read.read()
  core_read.close()
  # print "Before :\n",data_core_site 
  insert_core_site="<configuration><property><name>fs.default.name</name><value>hdfs://"+name_node_ip+":"+str(port)+"</value></property></configuration>"
  tmp=data_core_site.find("<configuration>")
  data_core_site=data_core_site[0:tmp]+insert_core_site
  # generating the core-site.xml file at the present directory
  core_write=open("/etc/hadoop/core-site.xml","w")
  core_write.write(data_core_site)
  #print data_core_site
  core_write.close()

def gen_hdfssite(n,send_ip_addr):
  #A function for developing hdfs-site.xml
  #Parameter n represents name node if, its value if 0 (zero), otherwise represents data node.
  #Parameter system_num helps in assigning different names to each data node system over the network.
  #Parameter add_manual_info holds addtitional data for hdfs-site.xml
  #Parameter send_ip_addr holds the ip address of the system where the file have to be send. 
  #Parameter password holds the root account password of the send_ip_addr system.

  hdfs_read=open("/etc/hadoop/hdfs-site.xml","r")
  # data_hdfs_site is a string containing all the data of hdfs-site.xml
  data_hdfs_site= hdfs_read.read()
  hdfs_read.close()
  if n==0:
    insert_hdfs_site="<configuration><property><name>dfs.name.dir</name><value>/systemhadoop</value></property>"    
  else:
    insert_hdfs_site="<configuration><property><name>dfs.data.dir</name><value>/systemhadoop</value></property>"
  #insert_hdfs_site=insert_hdfs_site+add_manual_info  
  insert_hdfs_site=insert_hdfs_site+"</configuration>"
  tmp=data_hdfs_site.find("<configuration>")
  data_hdfs_site=data_hdfs_site[0:tmp]+insert_hdfs_site
  # generating the hdfs-site.xml file at the present directory.
  hdfs_write=open("/etc/hadoop/hdfs-site.xml","w")
  hdfs_write.write(data_hdfs_site)
  #print data_hdfs_site
  hdfs_write.close()

def gen_mapredsite(jobtracker_ip,port=90001):
  #A function for developing  mapred-site.xml
  #Parameter jobtracker_ip holds the ip  address of job tracker.
  #Parameter port defines the port number for the service.
  #Parameter password holds the root account password of the send_ip_addr system.
  
  mapred_read=open("/etc/hadoop/core-site.xml","r")
  # data_mapred_site is a string containing all the data of mapred-site.xml
  data_mapred_site= mapred_read.read()
  mapred_read.close()
  # print "Before :\n",data_mapred_site 
  insert_mapred_site="<configuration><property><name>mapred.job.tracker</name><value>"+jobtracker_ip+":"+str(port)+"</value></property></configuration>"
  tmp=data_mapred_site.find("<configuration>")
  data_mapred_site=data_mapred_site[0:tmp]+insert_mapred_site
  # generating the mapred-site.xml file at the present directory
  mapred_write=open("/etc/hadoop/mapred-site.xml","w")
  mapred_write.write(data_mapred_site)
  #print data_mapred_site
  mapred_write.close()

###################################################################################################################################################

def gen_coresite2(name_node_ip,path):
  #A function for developing core-site.xml
  #Parameter name_node_ip holds the ip address of name node.
  #Parameter port defines the port number for the service.
  #Parameter send_ip_addr holds the ip address of the system where the file have to be send. 
  #Parameter password holds the root account password of the send_ip_addr system.
 
  core_read=open(path+"/core-site.xml","r")
  # data_core_site is a string containing all the data of core-site.xml
  data_core_site= core_read.read()
  core_read.close()
  # print "Before :\n",data_core_site 
  insert_core_site="<configuration><property><name>fs.default.name</name><value>hdfs://"+name_node_ip+":10001</value></property></configuration>"
  tmp=data_core_site.find("<configuration>")
  data_core_site=data_core_site[0:tmp]+insert_core_site
  # generating the core-site.xml file at the present directory
  core_write=open(path+"/core-site.xml","w")
  core_write.write(data_core_site)
  #print data_core_site
  core_write.close()

def gen_hdfssite2(n,path):
  #A function for developing hdfs-site.xml
  #Parameter n represents name node if, its value if 0 (zero), otherwise represents data node.
  #Parameter system_num helps in assigning different names to each data node system over the network.
  #Parameter add_manual_info holds addtitional data for hdfs-site.xml
  #Parameter send_ip_addr holds the ip address of the system where the file have to be send
  #Parameter password holds the root account password of the send_ip_addr system.
  hdfs_read=open(path+"/hdfs-site.xml","r")
  # data_hdfs_site is a string containing all the data of hdfs-site.xml
  data_hdfs_site= hdfs_read.read()
  hdfs_read.close()
  if n==0:
    insert_hdfs_site="<configuration><property><name>dfs.name.dir</name><value>file:/data/systemhadoop</value></property>"    
  else:
      insert_hdfs_site="<configuration><property><name>dfs.data.dir</name><value>file:/data/systemhadoop</value></property>"
  #insert_hdfs_site=insert_hdfs_site+add_manual_info  
  insert_hdfs_site=insert_hdfs_site+"</configuration>"
  tmp=data_hdfs_site.find("<configuration>")
  data_hdfs_site=data_hdfs_site[0:tmp]+insert_hdfs_site
  # overwriting the hdfs-site.xml file at the path
  hdfs_write=open(path+"/hdfs-site.xml","w")
  hdfs_write.write(data_hdfs_site)
  #print data_hdfs_site
  hdfs_write.close()

def gen_mapredsite2(jobtracker_ip,mapred_mode,path):
  #A function for developing  mapred-site.xml
  #Parameter jobtracker_ip holds the ip  address of job tracker.
  #Parameter port defines the port number for the service.
  #Parameter password holds the root account password of the send_ip_addr system.
  #Parameter mapred_mode defines the mode of map reduce.
  mapred_read=open(path+"/mapred-site.xml","r")
  # data_mapred_site is a string containing all the data of mapred-site.xml
  data_mapred_site= mapred_read.read()
  mapred_read.close()
  """#if mapred_mode==0:
    #insert_mapred_site="<configuration><property><name>mapred.job.tracker</name><value>"+jobtracker_ip+":"+port+"</value></property></configuration>"
  #if mapred_mode==1:
    #insert_mapred_site="<configuration><property><name>mapreduce.framework.name</name><value>local</value></property></configuration>"
  """
  if mapred_mode==2:
    insert_mapred_site="<configuration><property><name>mapreduce.framework.name</name><value>yarn</value></property></configuration>"
  tmp=data_mapred_site.find("<configuration>")
  data_mapred_site=data_mapred_site[0:tmp]+insert_mapred_site
  # generating the mapred-site.xml file at the present directory
  mapred_write=open(path+"/mapred-site.xml","w")
  mapred_write.write(data_mapred_site)
  #print data_mapred_site
  mapred_write.close()




def gen_yarnsite2(resource_manager_ip,mode,path):
  #A function for developing yarn-site.xml
  # Parameter resource_manager_ip contains the ip address of resource manager
  # Parameter mode defines the type of system, i.e. resource, node, client.
  yarn_read=open(path+"/yarn-site.xml","r")
  # data_yarn_site is a string containing all the data of core-site.xml
  data_yarn_site= yarn_read.read()
  yarn_read.close()
  # print "Before :\n",data_yarn_site 
  if mode==1: # resource manager
    insert_yarn_site="<configuration><property><name>yarn.resourcemanager.resource-tracker.address</name><value>hdfs://"+resource_manager_ip+":8025</value></property>"
    insert_yarn_site=insert_yarn_site+"<property><name>yarn.resourcemanager.scheduler.address</name><value>hdfs://"+resource_manager_ip+":8030</value></property></configuration>"
  if mode==2:  # node manager side
    insert_yarn_site="<configuration><property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>"
    insert_yarn_site=insert_yarn_site+"<property><name>yarn.resource-tracker.address</name><value>"+resource_manager_ip+":8025</value></property></configuration>"
  if mode==3: # client side
    insert_yarn_site="<configuration><property><name>yarn.resourcemanager.resource-tracker.address</name><value>hdfs://"+resource_manager_ip+":8025</value></property>"
    insert_yarn_site=insert_yarn_site+"<property><name>yarn.resourcemanager.scheduler.address</name><value>hdfs://"+resource_manager_ip+":8030</value></property></configuration>"
    insert_yarn_site=insert_yarn_site+"<property><name>yarn.resourcemanager.address</name><value>hdfs://"+resource_manager_ip+":8032</value></property></configuration>"
    
  tmp=data_yarn_site.find("<configuration>")
  data_yarn_site=data_yarn_site[0:tmp]+insert_yarn_site
  #status_report(recvr_ip,9995,"yarn data ---"+data_yarn_site)
  # generating the yarn-site.xml file at the present directory
  yarn_write=open(path+"/yarn-site.xml","w")
  yarn_write.write(data_yarn_site)
  #print data_yarn_site
  yarn_write.close()

###################################################################################################################################################

def status_report(server_ip,server_port,msg):
  s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
  s.sendto(msg,(server_ip,server_port))
  s.close()
###################################################################################################################################################

os.system("iptables -F")
os.system("systemctl stop firewalld")
os.system("setenforce 0")
try:
  list_pkgs=sys.argv;
  run_which=str(list_pkgs[1])
  run_which=int(run_which)
  mode_key=str(list_pkgs[2])
  client_py_ip=str(list_pkgs[3])
  present_dir=str(list_pkgs[4])
  keygen_mode=str(list_pkgs[5]) # keygen_mode , 0----password required, 1----- not required
  hadoop_mode=str(list_pkgs[6]) # 1----hadoop1,  2----- hadoop2 
  system_type=str(list_pkgs[7]) # n---name , d---data, j---job   t---task
  password=str(list_pkgs[8])
  recvr_ip=str(list_pkgs[9])
  name_node_ip=str(list_pkgs[10])
  job_tracker_ip=str(list_pkgs[11])
  path="/hadoop2/etc/hadoop"
  send_command=list_pkgs[12:]
  back_data=" ".join(list_pkgs)
  status_report(recvr_ip,9995," argument received  :"+back_data + " .")
  status_report(recvr_ip,9995," argument received recvr_ip  :"+recvr_ip + " client.py ip-------- ."+client_py_ip)
    
  if keygen_mode=="0":
    os.system("ssh-keygen -f /root/.ssh/f.rsa -t rsa -N ''")
    os.system("sshpass -p "+password+" ssh-copy-id -i f.rsa -o 'StrictHostKeyChecking no' root@"+recvr_ip) 
  
  status_report(recvr_ip,9995," client.py script started")
  #### I N S T A L L A T I O N #####
  
  if (system_type=="n") or (system_type=="c")or(system_type=="j") or( (system_type=="d") and (mode_key=="0") )or(( system_type=="t") and (mode_key=="1" )) :
    i=0
    status_report(recvr_ip,9995,"inside if")
    # loops for getting and installing the packages from the server.
    while i< len(send_command)-1:
      value=os.system("rpm -q "+send_command[i])
      if value!=0:
        #print "package to install---",send_command[i]
        status_report(recvr_ip,9995," downloading package  :"+send_command[i]+"-->"+ send_command[i+1] + " .") ##downloading
        if send_command[i].find("jdk")>=0:
          os.system("scp root@"+recvr_ip+":"+present_dir+"softwares/java/"+send_command[i+1]+" /tmp/")
          status_report(recvr_ip,9995," received package  :"+send_command[i]+"-->"+ send_command[i+1] + " .")   ##received
          os.system("cd /tmp/ ; rpm -ivh "+send_command[i+1]) ##java installed
          status_report(recvr_ip,9995," installed package  :"+send_command[i]+"-->"+ send_command[i+1] + " .")   ##installed
        elif send_command[i].find("hadoop")>=0:
          if hadoop_mode=="1":
            os.system("scp root@"+recvr_ip+":"+present_dir+"softwares/hadoop1/"+send_command[i+1]+" /tmp/")
            status_report(recvr_ip,9995," received package  :"+send_command[i]+"-->"+ send_command[i+1] + " .")  ##received
            os.system("cd /tmp/ ; rpm -ivh "+send_command[i+1]+" --replacefiles") ## hadoop1 installed
            status_report(recvr_ip,9995," installed package  :"+send_command[i]+"-->"+ send_command[i+1] + " .")  ##installed
          else:
            tst_hadoop=int(os.system("hadoop"))
            if tst_hadoop!=0:
              os.system("scp root@"+recvr_ip+":"+present_dir+"softwares/hadoop2/"+send_command[i+1]+" /tmp/")
              status_report(recvr_ip,9995," received package  :"+send_command[i]+"-->"+ send_command[i+1] + " .")  ##received
              tmp_hadoop=send_command[i+1]
              tmp_hadoop=tmp_hadoop[0:len(tmp_hadoop)-7]
              os.system("cd /tmp/ ; tar -xvzf "+send_command[i+1])
              os.system("cd /tmp/ ;mv "+tmp_hadoop+" /hadoop2")
              status_report(recvr_ip,9995," installed package  :"+send_command[i]+"-->"+ send_command[i+1] + " .")  ##installed
        else:
          actual_name=send_command[i+1]
          status_report(recvr_ip,9995," actual names ---------> "+ send_command[i+1] + " .")       ##received    
          if actual_name[len(actual_name)-4:]==".rpm": # if remaining package is a .rpm file
            status_report(recvr_ip,9995," only rpm files --------->"+ send_command[i+1] + " .")       ##received
            os.system("scp root@"+recvr_ip+":"+present_dir+"softwares/packages/"+send_command[i+1]+" /tmp/")        
            status_report(recvr_ip,9995," received package  :"+send_command[i]+"-->"+ send_command[i+1] + " .")       ##received
            os.system("cd /tmp/ ; rpm -ivh "+send_command[i+1]) # package installed
            status_report(recvr_ip,9995," installed .rpm  package  :"+send_command[i]+"-->"+ send_command[i+1] + " .")       ##installed
        os.system("rm -r -f /tmp/*") # to remove the packages send by servr.py for installation to /tmp/ 
      else:
        status_report(recvr_ip,9995," already installed :"+send_command[i]+"-->"+ send_command[i+1] + " .")       ## already installed
      i=i+2 
                                                         #### I N S T A L L A T I O N #####

                                #### F I L E   G E N E R A T I O N,   S T A R T I N G    S E R V I C E #####

  
  
  
  if hadoop_mode=="1": # if hadoop1 is selected.
    status_report(recvr_ip,9995,"starting-------hadoop mode =1------ service")
    if system_type=="n":
      status_report(recvr_ip,9995,"system type-n starting name node service")
      gen_coresite(name_node_ip,10001)
      gen_hdfssite(0,name_node_ip)
      os.system("cd /etc/hadoop/ ; hadoop namenode -format -force ; hadoop-daemon.sh start namenode")
      status_report(recvr_ip,9995,"system type-n running name node service")
      status_report(recvr_ip,9995,"completed")  

    elif system_type=="j":
      gen_coresite(name_node_ip,10001)
      gen_mapredsite(job_tracker_ip,9001)
      status_report(recvr_ip,9995,"system type-j starting job tracker service")
      os.system("cd /etc/hadoop/ ; hadoop-daemon.sh start jobtracker")
      status_report(recvr_ip,9995,"system type-j running job tracker service")
      status_report(recvr_ip,9995,"completed")  

    elif system_type=="d":
      gen_coresite(name_node_ip,10001)
      gen_hdfssite(1,name_node_ip)
      status_report(recvr_ip,9995,"system type-d starting data node service")
      os.system("cd /etc/hadoop/ ; hadoop-daemon.sh start datanode")
      status_report(recvr_ip,9995,"system type-d running data node service")
      if run_which>0:
        gen_mapredsite(job_tracker_ip,"9001")
        status_report(recvr_ip,9995,"system type-d starting task tracker service")
        os.system("cd /etc/hadoop/ ; hadoop-daemon.sh start tasktracker")
        status_report(recvr_ip,9995,"system type-d running tracker service")
      status_report(recvr_ip,9995,"completed")  
    
    else :
      gen_coresite(name_node_ip,10001)
      gen_mapredsite(job_tracker_ip,"9001")
      if run_which>0:
        gen_hdfssite(1,name_node_ip)
        status_report(recvr_ip,9995,"inside else --if-- starting data node service")
        os.system("cd /etc/hadoop/ ; hadoop-daemon.sh start datanode")
        status_report(recvr_ip,9995,"inside else --if-- running data node service")
      status_report(recvr_ip,9995,"inside else--starting task tracker service")
      os.system("cd /etc/hadoop/ ; hadoop-daemon.sh start tasktracker")
      status_report(recvr_ip,9995,"inside else--running task tracker service")
      status_report(recvr_ip,9995,"completed")  


                               #####   H A D O O P 2  ####
  if hadoop_mode=="2": # if hadoop2 is selected.
    ##### java path #####
    list_java=str(commands.getoutput("cd /usr/java/ ; ls"))
    list_java=list_java.split("\n")
    for i in list_java:
      j=str(i)
      if j.find("jdk")>=0:
        java_version=str(i)
    status_report(recvr_ip,9995,"hadoop 2--- java version = "+java_version)
    ##### java path #####
    status_report(recvr_ip,9995,"hadoop 2--- variables generating")
    add_data_bash="\nexport JAVA_HOME=/usr/java/"+java_version+"\nexport PATH=$JAVA_HOME/bin:$PATH\nexport HADOOP_HOME=/hadoop2\nexport PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
    status_report(recvr_ip,9995,"hadoop 2--- variables generated")
    status_report(recvr_ip,9995,"hadoop 2--- opening /root/.bash.rc for read")
    ##### environment variable set #####
    ft_bash=open("/root/.bashrc","r")
    ft_data=ft_bash.read()
    ft_bash.close()
    status_report(recvr_ip,9995,"hadoop 2--- closing /root/.bash.rc")
    if ft_data.find(add_data_bash) <0 :
      status_report(recvr_ip,9995,"hadoop 2--- opening /root/.bash.rc for append")
      f_bash=open("/root/.bashrc","a")
      f_bash.write(add_data_bash)
      f_bash.close()
      status_report(recvr_ip,9995,"hadoop 2--- closing /root/.bash.rc")
    os.system("cd /root/ ;  source ~/.bashrc")
    status_report(recvr_ip,9995,"hadoop 2--- variables successfully written to /root/.bashrc ")
    
    if system_type=="n":
      status_report(recvr_ip,9995,"system type-n generating core site")
      gen_coresite2(name_node_ip,path)
      status_report(recvr_ip,9995,"system type-n generated core site")
      status_report(recvr_ip,9995,"system type-n generating hdfs site")
      gen_hdfssite2(0,path)
      status_report(recvr_ip,9995,"system type-n generated hdfs site")
      status_report(recvr_ip,9995,"system type-n core and hdfs generated ")  
      status_report(recvr_ip,9995,"system type-n updating variables ")  
      os.system("cd /root/ ; source ~/.bashrc") 
      status_report(recvr_ip,9995,"system type-n updating variables ")
      status_report(recvr_ip,9995,"system type-n starting name node service")
      os.system("cd /hadoop2/etc/hadoop/ ; hdfs namenode -format -force ; hadoop-daemon.sh start namenode")      
      status_report(recvr_ip,9995,"system type-n running name node service")
      status_report(recvr_ip,9995,"completed")  
    
    elif system_type=="j":
      status_report(recvr_ip,9995,"system type-j generating core site")
      gen_coresite2(name_node_ip,path)
      status_report(recvr_ip,9995,"system type-j generated core site")
      status_report(recvr_ip,9995,"system type-j generating yarn site")
      gen_yarnsite2(job_tracker_ip,1,path)
      status_report(recvr_ip,9995,"system type-j generated yarn site")
      status_report(recvr_ip,9995,"system type-j updating variables ")  
      os.system("cd /root/ ; source ~/.bashrc") 
      status_report(recvr_ip,9995,"system type-j updated variables ")  
      status_report(recvr_ip,9995,"system type-j generated yarn site")
      status_report(recvr_ip,9995,"system type-j starting resource manager service")
      os.system("cd /root/ ; source ~/.bashrc")
      os.system("cd /hadoop2/etc/hadoop/ ; yarn-daemon.sh start resourcemanager")      
      status_report(recvr_ip,9995,"system type-j running resource manager service")
      status_report(recvr_ip,9995,"completed")  
    
    elif system_type=="d":
      gen_coresite2(name_node_ip,path)
      gen_hdfssite2(1,path)
      status_report(recvr_ip,9995,"system type-d starting data node service")
      os.system("cd /root/ ; source ~/.bashrc")
      os.system("cd /hadoop2/etc/hadoop/ ; hadoop-daemon.sh start datanode")
      status_report(recvr_ip,9995,"system type-d running data node service")
      if run_which>0:
        gen_yarnsite2(job_tracker_ip,2,path)
        status_report(recvr_ip,9995,"system type-d starting node manager service")
        os.system("cd /root/ ; source ~/.bashrc")
        os.system("cd /hadoop2/etc/hadoop/ ; yarn-daemon.sh start nodemanager")        
        status_report(recvr_ip,9995,"system type-d running node manager service")
      status_report(recvr_ip,9995,"completed")  
    
    else :
      gen_coresite2(name_node_ip,path)
      gen_yarnsite2(job_tracker_ip,2,path)
      if run_which>0:
        gen_hdfssite2(1,path)
        status_report(recvr_ip,9995,"elif--if--system type-d starting data node service")
        os.system("cd /root/ ; source ~/.bashrc")
        os.system("cd /hadoop2/etc/hadoop/ ; hadoop-daemon.sh start datanode")
        status_report(recvr_ip,9995,"elif--if--system type-d running data node service")
      status_report(recvr_ip,9995,"inside elif--starting node manager service")
      os.system("cd /root/ ; source ~/.bashrc")
      os.system("cd /hadoop2/etc/hadoop/ ; yarn-daemon.sh start nodemanager")        
      status_report(recvr_ip,9995,"inside elif--running node manager service")
      status_report(recvr_ip,9995,"completed")  


                                      #### F I L E   G E N E R A T I O N,   S T A R T I N G    S E R V I C E ##### 

except:
  status_report(recvr_ip,9995,"Error Occured at client.py of "+client_py_ip) 
