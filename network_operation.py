import os,commands,sys,re
def nmap_list():
  my_ip=str(commands.getoutput("hostname -i"))
  if_config_data=str(commands.getoutput("ifconfig |"+"grep "+my_ip))
  mask=re.findall('255\.[0-9]+\.[0-9]+\.[0-9]+',if_config_data)
  mask_list=mask[0].split(".")
  print mask_list
  count=0
  for i in mask_list:
    if i=='0':
      pos=count*8
      break
    count=count+1
  nmap_data=str(commands.getoutput('nmap -sP '+my_ip+'/'+str(pos)+' -n')) # network ip 
  ip_list=re.findall('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+',nmap_data)  # regular expression to find the remaining ip addresses.
  ip_list.remove(my_ip) # current system  ip will be removed
  return ip_list                    
