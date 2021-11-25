#DEPLOY: python3 DEPLOY.py HOSTS.txt
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
    
def mt(list_m, fct, *args ):
    threads = []
    for i in list_m:
        x = threading.Thread(target = fct, args = (i, *args))
        threads.append(x)
        x.start()

    for x in threads:
        x.join()

def deploy(i, pth):
    cmd = "ssh " + i + " mkdir -p "+pth+" && scp "+pth+"/SLAVE.py " + i + ":"+pth+"/SLAVE.py"
    cmdsh(cmd)
        
def main():
    f = open(sys.argv[1], 'r')
    hosts = [line.strip() for line in f]
    mt(hosts, deploy, '/tmp/sarda-20')
    print('DEPLOY FINISHED')        
               
if __name__ == '__main__':
    main()
