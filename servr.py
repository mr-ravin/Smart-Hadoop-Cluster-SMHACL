import os,sys,commands,socket
os.system("iptables -F")
os.system("systemctl stop firewalld")
os.system("setenforce 0")
####################################################################################################################################################

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


def gen_coresite2(name_node_ip,path):
  #A function for developing core-site.xml
  #Parameter name_node_ip holds the ip address of name node.
  #Parameter port defines the port number for the service.
  #Parameter send_ip_addr holds the ip address of the system where the file have to be send. 
  #Parameter password holds the root account password of the send_ip_addr system.
  mydir=commands.getoutput("pwd")
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


def gen_mapredsite2(jobtracker_ip,mapred_mode,path):
  #A function for developing  mapred-site.xml
  #Parameter jobtracker_ip holds the ip  address of job tracker.
  #Parameter port defines the port number for the service.
  #Parameter password holds the root account password of the send_ip_addr system.
  #Parameter mapred_mode defines the mode of map reduce.
  os.system("cd /hadoop2/etc/hadoop/ ; mv mapred-site.xml.template mapred-site.xml")
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


#####################################################################################################################################################
def status_report(server_ip,server_port,msg):
  s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
  s.sendto(msg,(server_ip,server_port))
  s.close()
def servr(run_which,mode_key,keygen_mode,hadoop_mode,system_type,password,recvr_ip,name_node_ip,job_tracker_ip,client_py_ip,is_client):
 try:
  #print "client.py send --------",client_py_ip
  hadoop_package=""
  file_name=str(commands.getoutput("cd softwares/packages/ ;ls"))
  packages_name=file_name.split("\n")
  #print "package name---------",packages_name
  java_package=str(commands.getoutput("cd  softwares/java/ ;ls"))
  java_package=java_package.split("\n")
  java_package=java_package[0]
  if hadoop_mode=="1": # for hadoop1.
    hadoop_package=str(commands.getoutput("cd softwares/hadoop1/ ;ls"))
  if hadoop_mode=="2":  #for hadoop2.
    hadoop_package=str(commands.getoutput("cd softwares/hadoop2/ ;ls"))
  #print "hadoop package---------",hadoop_package

  
  list_pkgs=[]
  list_pkgs.append(java_package)
  list_pkgs.append(hadoop_package)
  list_pkgs=list_pkgs+packages_name
  send_command=[]
  #print " before java,list_pkgs------------",list_pkgs
  tmp_list=[]
  chk_list=["jdk","hadoop","hive","docker","pig","splunk"]
  for i in list_pkgs :
    for j in chk_list :
      if i.find(j)>=0 :
        send_command.append(j)
        send_command.append(i)
        tmp_list.append(i)
  list_pkgs=tmp_list[:]
  #print "list packages,----------after loop---------- ",list_pkgs
  #print "-----send_command---------",send_command
  
  #### C L I E N T  S I D E   I N S T A L L A T I O N  #####
  if is_client==1:
    #print "client----------",is_client
    status_report(recvr_ip,9995," servr.py started")
    i=0
    #print "total ------",send_command
    while i< len(send_command)-1:
      value=os.system("rpm -q "+send_command[i])
      if value!=0:
        #print "package to install ---",send_command[i+1]
        status_report(recvr_ip,9995," installing package  :"+ send_command[i+1] + " .")
        #print "outer actual package name===",send_command[i+1]
        #print "alll------",send_command
        actual_name=send_command[i+1]  ##checked
        #print "outer actual name=",actual_name
        if actual_name[len(actual_name)-6:].find("tar.gz")>=0:
          #print "tar file----",actual_name 
          if actual_name.find("hadoop")>=0:
            if hadoop_mode=="2":
              #print "hadoop tar actual name---",actual_name
              os.system("mkdir extract")
              os.system("cp softwares/hadoop2/"+actual_name+" extract/")
              os.system("cd extract/ ; tar -xzvf "+str(actual_name))
              new_hadoop_name=actual_name[0:len(actual_name)-7] #new_hadoop_name
              #print "new hadoop = ",new_hadoop_name
              #######   path variables#######
              os.system("cd ./extract/ ; mv "+new_hadoop_name+ " /hadoop2")
              #print "hadoop 2 installed"
        else:
          if send_command[i]=="hadoop": # installing hadoop
            status_report(recvr_ip,9995,"installing : "+ send_command[i] + " .")
            os.system("cd ./softwares/hadoop1/ ; rpm -ivh "+str(actual_name)+" --replacefiles")
            status_report(recvr_ip,9995,"installed : "+ send_command[i] + " .")
          else: # installing other packages
            status_report(recvr_ip,9995,"installing : "+ send_command[i] + " .")
            os.system("cd ./softwares/packages/ ; rpm -ivh "+str(actual_name))
            status_report(recvr_ip,9995,"installed : "+ send_command[i] + " .")                           #installed
        status_report(recvr_ip,9995,"installed : "+ send_command[i]+ " .")
      else:
        #print "package  is already installed ",send_command[i+1]
        #print "already installed ",send_command[i]," package--- ",send_command[i+1]
        status_report(recvr_ip,9995," already exist  : "+ send_command[i] + " .")
      i=i+2
    #status_report(recvr_ip,9995,"installation finished")
    #### C L I E N T  S I D E   I N S T A L L A T I O N  #####
    #### S E T U P     C O R E - S I T E . X M L   #####
    #status_report(recvr_ip,9995,"configuring core-site.xml") #checked
    if hadoop_mode=="1":
      gen_coresite(name_node_ip,10001)
    if hadoop_mode=="2":
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
      status_report(recvr_ip,9995,"hadoop 2--- opening /root/.bash.rc for append")
      f_bash=open("/root/.bashrc","a")
      f_bash.write(add_data_bash)
      f_bash.close()
      status_report(recvr_ip,9995,"hadoop 2--- closing /root/.bash.rc")
      os.system("cd /root/ ;  source ~/.bashrc")
      status_report(recvr_ip,9995,"hadoop 2--- variables successfully written to /root/.bashrc")
      ############generating sites #################
      os.system("cd /root/ ; source ~/.bashrc")
      status_report(recvr_ip,9995,"hadoop 2--- generating core site")
      gen_coresite2(name_node_ip,"/hadoop2/etc/hadoop")
      status_report(recvr_ip,9995,"hadoop 2--- generating mapred site")
      gen_mapredsite2(job_tracker_ip,2,"/hadoop2/etc/hadoop")
      status_report(recvr_ip,9995,"hadoop 2--- generating yarn site")
      gen_yarnsite2(job_tracker_ip,3,"/hadoop2/etc/hadoop")
      ############generated sites #################
      # do it here
 
    #### S E T U P     C O R E - S I T E . X M L   #####
  
  if system_type!="c":
    othr_commands=[]
    dirctry=str(commands.getoutput("pwd"))+"/"
    #print "directry ---------",dirctry
    run_which=str(run_which)
    othr_commands.append(run_which)
    othr_commands.append(mode_key)
    othr_commands.append(client_py_ip)
    othr_commands.append(dirctry)
    othr_commands.append(keygen_mode)
    othr_commands.append(hadoop_mode)
    othr_commands.append(system_type)
    othr_commands.append(password)
    othr_commands.append(recvr_ip)
    othr_commands.append(name_node_ip)
    othr_commands.append(job_tracker_ip)
    send_command=othr_commands+send_command
    send_command=" ".join(send_command)

    #print "-----------------------send_command----------\n",send_command
    #print "\n\nkeygen_mode",keygen_mode
    #print "client_py====",client_py_ip
    #if keygen_mode=="0":  # to automate keygen transfer.      
      #os.system("sshpass -p "+password+" ssh-copy-id  -o 'StrictHostKeyChecking no' root@"+client_py_ip) # client_py_ip
    #print "send command----------------",send_command
    status_report(recvr_ip,9995,"sending client.py file")
    os.system("scp client.py root@"+client_py_ip+":/tmp/ ") 
    os.system("ssh root@"+client_py_ip+" \" cd /tmp/ ; python client.py "+send_command+"\" &")
    status_report(recvr_ip,9995," client.py file sended")
    status_report(recvr_ip,9995,"finished----servr.py ") #checked
  #print system_type,"executing one machine ",client_py_ip
 except:
   status_report(recvr_ip,9995,"ERROR in servr.py ") #checked
