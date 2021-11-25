#SLAVE
import time
import sys
import subprocess
import threading
from os import listdir
from os import path
import pandas as pd
from pandas.util import hash_pandas_object

# variables permanentes:
pth = '/tmp/sarda-20/'
hostname = subprocess.check_output("hostname", shell=True, universal_newlines=True).strip()

machines = open(pth+'machines.txt', 'r')
vhosts = [line.strip() for line in machines]
machines.close()

# fonction de commande de shell
def cmdsh(cmd):
    try:
        output = subprocess.check_output(cmd, shell=True, universal_newlines=True, stderr= subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        print(hostname + " Status : FAIL", exc.returncode, exc.output)
        return False
    else:
        return True

    
def mt(list_m, fct, *args ):
    threads = []
    for i in list_m:
        x = threading.Thread(target = fct, args = (i, *args))
        threads.append(x)
        x.start()

    for x in threads:
        x.join()
        
#fonction de map
def operation_map():
    cmdsh("mkdir -p "+pth+"maps")
    for f in listdir(pth+"splits/"):
        input_file = open(pth+'splits/'+f, 'r',  errors='ignore')
        count_list = [[x for x in line.strip().split(" ") if x not in ['','\n','\t',]] for line in input_file]
        input_file.close() 
        output_filename = pth+"maps/UM" + f
        output_file = open(output_filename, 'w')
        ll = ''
        for line in count_list:
            for word in line: 
                ll += word.lower().strip()+"\n"
        output_file.write(ll)
        output_file.close()

def send_shuffle(mach):
    if hostname == mach:
        cmd= "cp "+pth+"shuffles/"+hostname+mach+".txt "+pth+"shufflesreceived/"
        cmdsh(cmd)
    else:
        cmd = "scp "+pth+"shuffles/"+hostname+mach+".txt "+mach+":"+pth+"shufflesreceived/"+hostname+mach+".txt"
        cmdsh(cmd)
    
def operation_shuffle():
    cmd = "mkdir -p "+pth+"shuffles "+pth+"shufflesreceived"
    cmdsh(cmd)
    
    #création des hashfiles
    for u in listdir(pth+"maps/"):
        input_file = open(pth+"maps/" + u, 'r')
        df = pd.DataFrame(input_file.readlines())
        df["hash"] = pd.util.hash_array(df[0])
        df["hash"] = df["hash"].apply(lambda x: x%len(vhosts))
        
        #création de fic
        for i in range(len(vhosts)):
            dfAsString = df.loc[df["hash"]==i, 0].to_string(header=False, index=False)
            f = open(pth+'/shuffles/'+hostname+vhosts[i]+".txt", 'a')
            f.write(dfAsString)
            f.close()
    
    #envoi en MT
    mt(vhosts, send_shuffle)

def mapred_emergency(lis_spl):
    lis_spl = lis_spl.split(";")
    for f in lis_spl:
        input_file = open(pth+'splits/'+f, 'r',  errors='ignore')
        count_list = [[x for x in line.strip().split(" ") if x not in ['','\n','\t',]] for line in input_file]
        input_file.close() 
        output_filename = pth+"maps/UM" + f
        output_file = open(output_filename, 'w')
        ll = ''
        for line in count_list:
            for word in line: 
                ll += word.lower().strip()+"\n"
        output_file.write(ll)
        output_file.close()
    
    for u in lis_spl:
        input_file = open(pth+"maps/UM" + u, 'r')
        df = pd.DataFrame(input_file.readlines())
        df["hash"] = pd.util.hash_array(df[0])
        df["hash"] = df["hash"].apply(lambda x: x%len(vhosts))
        
        #création de fic
        for i in range(len(vhosts)):
            dfAsString = df.loc[df["hash"]==i, 0].to_string(header=False, index=False)
            f = open(pth+'/shuffles/'+hostname+vhosts[i]+".txt", 'a')
            f.write(dfAsString)
            f.close()

def resend_shuff():
    #envoi en MT
    mt(vhosts, send_shuffle)
    
    
    
def reduce():
    hf = [f for f in listdir(pth+"shufflesreceived")]
    cmdsh("mkdir -p "+pth+"reduces")
    
    word_count = {}  
    for i in hf :
        file = open(pth+"shufflesreceived/"+i, 'r')
        for line in file:
            word = line.strip().lower()
            if not word in word_count:
                word_count[word] = 1
            else:
                word_count[word] = word_count[word] + 1
        file.close()
    

    phrase=""
    for key, value in word_count.items():
        phrase+=key+" "+str(value)+"\n"
    filename = pth+"reduces/"+hostname+".txt"
    red = open(filename, 'w')
    red.write(phrase)
    red.close()

def reaffect(chang):
    a = chang.split(";")
    cmd = "cat "+pth+"shuffles/"+hostname+a[1]+".txt >> "+pth+"shuffles/"+hostname+a[0]+".txt"
    print(cmd)
    cmdsh(cmd)
    
def main(step):
    
    # map:
    if step == '0':
        # python3 SLAVE.py 0 
        operation_map()
    
    # shuffle
    elif step == '1':
        # python3 SLAVE.py 1 
        operation_shuffle()
    
    # shuffle emergency
    elif step == '11':
        # python3 SLAVE.py 11 mach1;mach2;... 
        if sys.argv[2] != None:
            mapred_emergency(sys.argv[2])
    
    elif step == '12':
        # python3 SLAVE.py 11 mach_ok;mach_crash 
        reaffect(sys.argv[2])
        
    elif step == '13':
        # python3 SLAVE.py 11 mach_ok;mach_crash 
        resend_shuff()
    
    # reduce
    elif step == '2':
        # python3 SLAVE.py 2
        reduce()
    
if __name__ == '__main__':
    main(sys.argv[1])
