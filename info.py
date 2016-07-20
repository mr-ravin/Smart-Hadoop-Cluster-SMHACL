import commands,socket,sys,re
def status_report(server_ip,server_port,msg):
  s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
  s.sendto(msg,(server_ip,server_port))
  s.close()
dt=sys.argv

#### calculate free RAM and Secondary memory ####

server_ip=str(dt[1])
tmp_mem=commands.getoutput("free") ## for getting data related to RAM.
w=tmp_mem.split("\n")
mem_data=w[1] # string that contain information.
# implementing regular expression for obtaining RAM information. (free space)
mem_tmp1=re.findall('S*[0-9]+',mem_data)
mem=mem_tmp1[2]  ## free RAM in the system.
tmp_hd=commands.getoutput("df") ## for getting data related to HDD.
w=tmp_hd.split("\n")
hd_data=w[1]  # string that contain information.
# implementing regular expression for obtaining hdd information.(free space)
hd_tmp1=re.findall('S*[0-9]+',hd_data)
hd=hd_tmp1[2]  ## free Secondary Memory in the system.
send="Free RAM "+mem+",Free HDD "+hd+":completed"
status_report(server_ip,9999,send)

