#CLEAN: python3 CLEAN.py HOSTS.txt
import sys
import subprocess
import threading

def cmdsh(cmd):
    try:
        output = subprocess.check_output(cmd, shell=True, universal_newlines=True, stderr= subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        print("Status : FAIL", exc.returncode, exc.output)
        return False
    else:
        return True

def clean(i, pth):
    cmd = "ssh " + i + " rm -rf "+pth
    cmdsh(cmd)
    
def mt(list_m, fct, *args ):
    threads = []
    for i in list_m:
        x = threading.Thread(target = fct, args = (i, *args))
        threads.append(x)
        x.start()

    for x in threads:
        x.join()
        
def main():
    # suppression des fichiers du master
    cmd = "cd /tmp/sarda-20 && rm -rf machines.txt output/ splits/"
    cmdsh(cmd)
    
    # suppression en MT des dossiers sur les Slaves
    f = open(sys.argv[1], 'r')
    hosts = [line.strip() for line in f]
    try: 
        mt(hosts, clean, '/tmp/sarda-20')
        print('CLEAN FINISHED')
    except:
        print('CLEAN FAILED')

if __name__ == '__main__':
    main()
