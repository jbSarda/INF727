#MASTER: python3 MASTER.py HOSTS.txt NBSPLITS
import time
import sys
import subprocess
import threading
from os import listdir
from os import path
from os import rename
import tempfile
from itertools import zip_longest

pth = '/tmp/sarda-20/'
error = []

def cm(cmd):
    try:
        output = subprocess.check_output(cmd, shell=True, universal_newlines=True, stderr= subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        return False
    else:
        return True

def cmdsh(cmd):
    if cm(cmd) == False:
        print("\n\n----------->>>PROCESS ERROR: " + cmd)
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

def verif_connect(machine, list_mach):
    try:
        output = subprocess.check_output("ssh " + machine + " exit", shell=True, universal_newlines=True)
        list_mach.append(machine)
    except:
        print(machine+': connexion failed')

def grouper(n, iterable, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)
    
def send_machines(mach, file):
    cmd = "scp "+file+" " + mach + ":" + file  
    cmdsh(cmd)    

def send_split(mach, dic):
    cmd="ssh "+mach+" mkdir -p "+pth+"splits/ && scp "+" ".join([pth+"splits/"+i for i in dic[mach]])+" "+mach+":"+pth+"splits/"
    cmdsh(cmd)
    cmd="ssh "+mach+" python3 "+pth+"SLAVE.py 0 "
    cmdsh(cmd)

def shuff(mach):
    cmd = "ssh "+ mach + " python3 "+pth+"SLAVE.py 1"
    if cmdsh(cmd) == False:
        print('----------->>>Error in shuffle function: ' + mach)
        error.append(mach)
        
def shuff_pb(mach, dic):
    if len(dic[mach])>0:
        cmd = "ssh "+ mach + " python3 "+pth+"SLAVE.py 11 "+";".join(dic[mach])
        if cmdsh(cmd) == False:
            print('----------->>>Error in shuffle function: ' + mach)
            error.append(mach)
    
def red(mach):
    cmd = "ssh "+ mach + " python3 "+pth+"SLAVE.py 2"
    if cmdsh(cmd) == False:
        print('----------->>>Error in reduce function: ' + mach)
        error.append(mach)
    
def centr_loc(mach):
    cmd = "scp " + mach + ":"+pth+"reduces/*.txt "+pth+"output/"
    cmdsh(cmd)

def sup(mach, lis_pb):
    for i in lis_pb:
        u = " \"find "+pth+"shufflesreceived/ -name '"+i+"*' -exec rm {} \\;\""
        cmdsh("ssh "+ mach +u)

def reaffect_red(mach, lis):
    for i in lis: 
        cmd = "ssh "+mach+" \"python3 "+pth+"SLAVE.py 12 '"+";".join(i)+"'\""
        cmdsh(cmd)

def resend(mach):
    cmdsh("ssh "+ mach + " python3 "+pth+"SLAVE.py 13")
        
def main():
    
    p1 = time.time()  
    print('connection / déploiement -- ...\r', end='')
    cmdsh('python3 CLEAN.py HOSTS.txt')
    cmdsh('python3 DEPLOY.py HOSTS.txt')
    
    # test de connection, création fic machines.txt et envoi
    f = open(sys.argv[1], 'r')
    hosts = [line.strip() for line in f]
    f.close()
    vhosts = []
    mt(hosts, verif_connect, vhosts)
    f = open(pth+'machines.txt', 'w')
    for i in vhosts:
        f.write(i+"\n")
    f.close()
    
    # envoi aux machines
    mt(vhosts, send_machines, '/tmp/sarda-20/machines.txt')  
    p2 = time.time()
    print('connection / déploiement -- %2.2fs' % (p2-p1))
    print(f'\t{len(vhosts)} machines disponibles sur {len(hosts)}')
    if len(vhosts)<len(hosts):
        print('machines indispo: ', " ".join([i for i in hosts if i not in vhosts]))
    
    #split de l'input et envoi
    nb = int(sys.argv[2]) 
    print('MAPPING -- ...\r', end='')
    cmd = "mkdir -p "+pth+"splits && split -n " + str(len(vhosts)*nb)+" "+pth+"input/input.txt && mv x* "+pth+"splits"
    cmdsh(cmd)
    splits = [f for f in listdir(pth+"splits/")]
    dic_splits={vhosts[i]:[splits[j] for j in range(len(splits)) if j%len(vhosts) == i] for i in range(len(vhosts))}
    mt(vhosts, send_split, dic_splits)
    p3 = time.time()
    print('MAP FINISHED -- %2.2fs' % (p3-p2))
    
    #SHUFFLE
    print('SHUFFLING -- ...\r', end='')
    mt(vhosts, shuff)
    
    pb = [i for i in error if i!= None]
    
    # traitement d'un problème de crash machine
    if len(pb)>0:
        print('----------->>>répartition des splits de ' + " ".join(pb))
        #sys.exit()
        # changement de la liste machines
        vhosts= [i for i in vhosts if i not in pb]
        f = open(pth+'machines.txt', 'w')
        for i in vhosts:
            f.write(i+"\n")
        f.close()
        mt(vhosts, send_machines, '/tmp/sarda-20/machines.txt') 
        
        # répartition des splits des workers crashés
        split_sup = []
        for i in pb:
            split_sup += dic_splits[i]
        dic_splits={vhosts[i]:[split_sup[j] for j in range(len(split_sup)) if j%len(vhosts) == i] for i in range(len(vhosts))}
        vhosts_conc = [i for i in dic_splits if dic_splits[i] != []]
        mt(vhosts_conc, send_split, dic_splits)
        
        #suppression des splits des machines crashées
        print('----------->>>suppression des shuffles envoyés par '+ " ".join(pb))
        mt(vhosts, sup, pb)
        
        print('----------->>>reprise shuffle')
        mt(vhosts_conc, shuff_pb, dic_splits)
        
        # réaffectation des taches de reduce
        mach_red = [[vhosts[i], pb[i]] for i in range(len(pb))]
        for i in mach_red:
            print('----------->>>réaffectation du reduce '+ i[1]+' -> '+i[0])
        mt(vhosts, reaffect_red, mach_red)
        
        # renvoi des shuff
        mt(vhosts, resend)
    
    p4 = time.time()
    print('SHUFFLE FINISHED -- %2.2fs' % (p4-p3))
    
    #REDUCE
    print('REDUCING -- ...\r', end='')
    mt(vhosts, red)
    p5 = time.time()
    print('REDUCE FINISHED -- %2.2fs' % (p5-p4))
    
    #Centralisation local
    print('CENTRALIZING -- ...\r', end = '')
    cmdsh('mkdir '+pth+'output')
    mt(vhosts, centr_loc)
    cmd = "cat "+pth+"output/*.txt > "+pth+"output/output && rm -rf "+pth+"output/*.txt"
    cmdsh(cmd)
    cmd = "mv "+pth+"output/output "+pth+"output/output.txt"
    cmdsh(cmd)
    ff = open(""+pth+"output/output.txt", 'r+')
    dic = sorted([line.split(" ") for line in ff.readlines()], key = lambda x : int(x[1]), reverse = True)
    ff.seek(0)
    for i in dic:
        ff.write(" ".join(i))
    ff.close()
    p6 = time.time()
    print('RESULT COMPILED -- TOTAL TIME: %2.2fs\n\n' % (p6-p1))
    print('\tin output/output.txt :\n')
    print("".join([" ".join(i) for i in dic[:10]]))
    print('......\n')
    print("".join([" ".join(i) for i in dic[-10:]]))
    
if __name__ == '__main__':
    main()
