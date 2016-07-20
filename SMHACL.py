from network_operation import *
import socket
import servr
def read_choice():# to read choice of user from file
  res_data=open("res.txt","r")
  choice=res_data.read()
  res_data.close()
  return choice
os.system("iptables -F")
os.system("systemctl stop firewalld")
os.system("setenforce 0")
os.system("dialog --msgbox \"        SMART HADOOP CLUSTER  \n              SMHACL\n\n A Smart Tool for Building\n Clusters for Hadoop. \n\n\n\n\n\n\n\n\n\n                      created by--\n                      Ravin Kumar.\" 21 40")
network_test=str(commands.getoutput("hostname -i"))
val=network_test.find("Temporary")
if val<0: # to check network connection.
  os.system("dialog --menu \"Select Installation Mode :\" 10 30 2 1 \"Auto Installation.\" 2 \"Manual Installation. \" 2>res.txt")
  choice=int(read_choice())
  ip_list=[]
  ip_list=nmap_list()
  recvr_ip=network_test
  total_number_computer=len(ip_list) ## total connected computers.
             #### L O G  S E R V E R   ####
  log_s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
  log_s.bind((recvr_ip,9995))
             #### L O G  S E R V E R   ####
  #print "ip list--------------",ip_list
  os.system("dialog --menu \"Select Cluster Type :\" 10 30 2 1 \"Hadoop 1.\" 2 \"Hadoop 2. \" 2>res.txt")
  hadoop_mode=str(read_choice()) # hadoop type selected
  os.system("dialog --msgbox \"Total "+str(total_number_computer+1)+" computers available .\n  \" 10 30") # msg display.
  if total_number_computer<=2:
    os.system("dialog --msgbox \"Cluster can not be built,\nless computers available.\nadd more computers. \" 10 30") # msg display.
    sys.exit()
  os.system("dialog --menu \"Authentication Required :\" 10 30 2 1 \"YES.\" 2 \"NO. \" 2>res.txt") ### keygen mode 
  keygen_mode=int(read_choice())-1
  keygen_mode=str(keygen_mode)
  password="redhat" # default password.
  if keygen_mode=="0":
    os.system("dialog --inputbox \"Enter Network Password :\" 10 30  2>res.txt")
    password=str(read_choice())  ###  password assigned.
  if choice==1: #### A U T O   I N S T A L L A T I O N   M O D E ####
    name_node_ip=ip_list[0]# ip address of name node.
    job_tracker_ip=ip_list[1]# ip address of job tracker.

                #### A U T O   I N S T A L L A T I O N   M O D E ####

  if choice==2:  #### M A N U A L   I N S T A L L A T I O N   M O D E ####

    ##### memory status server ######
    stat_s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    stat_s.bind((recvr_ip,9999))
    stat_chk=0
    stat_cmnd=""
    dictnory={}
    tmp_ip=str(commands.getoutput("hostname -i"))
    for i in ip_list:
      os.system("scp info.py root@"+i+":/tmp/")
      os.system("ssh root@"+str(i)+" \"cd /tmp/ ; python info.py "+tmp_ip+"\"")
    while stat_chk < total_number_computer:
      stat_cmd=stat_s.recvfrom(500)
      stat_cmnd=str(stat_cmd[0])
      stat_cmnd_fin=stat_cmnd.split(":")
      stat_cmnd_1=stat_cmnd_fin[0] # memory data
      stat_cmnd_2=stat_cmnd_fin[1] # command
      if stat_cmnd_2=="completed":
        stat_chk=stat_chk+1  
      dictnory[str(stat_cmd[1][0])]=stat_cmnd_1
    stat_s.close()
    ##### memory status server ######
    cnt=1
    here_data=""
    for i in ip_list:
      if cnt==1:
        here_data=here_data+" "+str(str(cnt)+" \" "+i+"  "+dictnory[str(i)]+" \"") #"+"on " )
      else:
        here_data=here_data+" "+str(str(cnt)+" \" "+i+"  "+dictnory[str(i)]+" \"")# "+"off " )
      cnt=cnt+1
    #### data to display is generated ####
    chd="dialog --menu \"Select Name Node: \" 15 70 50 "+here_data +" 2>res.txt"  ### select name node.
    #print chd
    os.system(chd)
    here_choice=int(read_choice())-1 # menu begin from 1 ,  list index from zero.
    #print "name node ",here_choice
    name_node_ip=ip_list[here_choice] # name_node_ip.
    #print "name node-----",name_node_ip
    rem_ip_list=[]
    rem_ip_list=ip_list[:]
    rem_ip_list.remove(name_node_ip) # remaining ip for job tracker.
    #print "remaining ip : ",rem_ip_list
    #print "ip list --------",ip_list
    cnt=1
    here_data=""
    for i in rem_ip_list:
      if cnt==1:
        here_data=here_data+" "+str(str(cnt)+" \" "+i+"  "+dictnory[str(i)]+" \"")# "+"on " )
      else:
        here_data=here_data+" "+str(str(cnt)+" \" "+i+"  "+dictnory[str(i)]+" \"")# "+"off " )
      cnt=cnt+1
    chd="dialog --menu \"Select Job Tracker : \" 15 70 50 "+here_data +" 2>res.txt"  ### select name node.
    #print chd
    os.system(chd)
    here_choice=int(read_choice())-1  # menu begin from 1, list index from zero.
    job_tracker_ip=rem_ip_list[here_choice]  # job_tracker_ip.
    #print "job tracker-------", job_tracker_ip
    rem_ip_list.remove(job_tracker_ip)
    #print "remaining ip -----",rem_ip_list
                  #### M A N U A L   I N S T A L L A T I O N   M O D E ####
            
  os.system("dialog --inputbox \" Enter Number of Data \n Node: \" 10 30  2>res.txt")
  data_node=int(read_choice()) # number of data nodes.
  total_number_computer=len(ip_list)
  remain_pc=total_number_computer-2
  if (data_node > remain_pc) or (remain_pc<1):
    os.system("dialog --msgbox \" You Do not have much\n Computers in the network\n to fullfill your request.\" 21 30")
    sys.exit() #close 
  os.system("dialog --inputbox \" Enter Number of Task \n Tracker: \" 10 30  2>res.txt")
  task_tracker=int(read_choice()) # number of task tracker
  if (task_tracker > remain_pc) or (remain_pc<1) :
    os.system("dialog --msgbox \" You Do not have much\n Computers in the network\n to fullfill your request.\" 21 30")
    sys.exit() #close    
  rem_ip_list=ip_list[:]
  rem_ip_list.remove(name_node_ip)
  rem_ip_list.remove(job_tracker_ip)
  ip_pool=rem_ip_list[:] # remaining ips 
  if data_node>=task_tracker:
    mode_key="0"
    run_which=task_tracker
  else:
    mode_key="1"
    run_which=data_node

  client_py_ip =str(commands.getoutput("hostname -i"))
  servr.servr(str(run_which),mode_key,keygen_mode,hadoop_mode,"c",password,recvr_ip,name_node_ip,job_tracker_ip,client_py_ip,1) ### client --checked
  servr.servr(str(run_which),mode_key,keygen_mode,hadoop_mode,"n",password,recvr_ip,name_node_ip,job_tracker_ip,name_node_ip,0) ### name node  
  servr.servr(str(run_which),mode_key,keygen_mode,hadoop_mode,"j",password,recvr_ip,name_node_ip,job_tracker_ip,job_tracker_ip,0) ### job tracker  
  for i in range(0,data_node):
    if mode_key=="0":
      servr.servr(str(run_which),mode_key,keygen_mode,hadoop_mode,"d",password,recvr_ip,name_node_ip,job_tracker_ip,ip_pool[i],0) ### data node operation
      run_which=run_which-1

  for i in range(0,task_tracker):
    if mode_key=="1":
      servr.servr(str(run_which),mode_key,keygen_mode,hadoop_mode,"t",password,recvr_ip,name_node_ip,job_tracker_ip,ip_pool[i],0) ### task tracker operation.
      run_which=run_which-1
#  os.system("firefox "+name_node_ip+":50070")
#  os.system("firefox "+job_tracker_ip+":50030")
  
                #### L O G  S E R V E R   ####
  #print "total computer :----------",total_number_computer

  log_chk=0
  log_cmnd=""
  log_strng=""
  while log_chk < total_number_computer:
    log_cmd=log_s.recvfrom(500)
    log_cmnd=str(log_cmd[0])
    if log_cmnd=="completed":
      log_chk=log_chk+1 
      #print "registry_server : completed",chk
    log_strng="status = "+str(log_cmd[0])+"   on port: "+str(log_cmd[1][1]) 
    log_file_log=open("log_file.txt","a")
    log_date=commands.getoutput("date")
    log_file_log.write(log_date+"-----> ip_address : "+str(log_cmd[1][0])+"  "+log_strng+"\n")
    log_file_log.close()
  log_s.close()
  
                   #### L O G  S E R V E R   ####
  os.system("dialog --msgbox \" Completed \n please see log file.\n Name Node:\n "+name_node_ip+"\n Job Tracker:\n "+job_tracker_ip+"\" 10 30") 
  os.system("dialog --menu \"Select Operation Mode :\" 10 30 4 1 \"Upload File.\" 2 \"View Content. \" 3 \"Create Directory. \" 4 \"Exit. \" 2>res.txt")
  op_ch=int(read_choice())
  if hadoop_mode=="1":
    if op_ch==1:
      os.system("dialog --inputbox \" Enter File name :\" 10 30  2>res.txt")
      filename=str(read_choice())
      os.system("cd /etc/hadoop/ ; hadoop fs -put "+filename+" /")
    elif op_ch==2:
      os.system("cd /etc/hadoop/ ; hadoop fs -ls /")
    elif op_ch==3:
      os.system("dialog --inputbox \" Enter Directory Name :\" 10 30  2>res.txt")
      input_dir=str(read_choice())
      os.system("cd /etc/hadoop/ ; hadoop fs -mkdir /"+input_dir) 
    else:
      sys.exit()
  
  if hadoop_mode=="2":
    if op_ch==1:
      os.system("dialog --inputbox \" Enter File name :\" 10 30  2>res.txt")
      filename=str(read_choice())
      os.system("cd /hadoop2/etc/hadoop/ ; hadoop fs -put "+filename+" /")
    elif op_ch==2:
      os.system("cd /hadoop2/etc/hadoop/ ; hadoop fs -ls /")
    elif op_ch==3:
      os.system("dialog --inputbox \" Enter Directory Name :\" 10 30  2>res.txt")
      input_dir=str(read_choice())
      os.system("cd /etc/hadoop/ ; hadoop fs -mkdir /"+input_dir) 
    else:
      sys.exit()
else:
  os.system("dialog --msgbox \" NETWORK CONNECTION ERROR. \n\n\n Please check your \n network connectivity.\" 10 30")
