
import threading, time, csv
from pathlib import Path

class SemaphoreRW:
    def __init__(self):
        self.mutex=threading.Semaphore(1); self.resource=threading.Semaphore(1); self.readers=0
    def read(self,n,data,log):
        self.mutex.acquire(); self.readers+=1
        if self.readers==1: self.resource.acquire()
        self.mutex.release()
        log.append((n,"READ",data[0])); time.sleep(.02)
        self.mutex.acquire(); self.readers-=1
        if self.readers==0: self.resource.release()
        self.mutex.release()
    def write(self,n,data,log):
        self.resource.acquire(); data[0]+=1
        log.append((n,"WRITE",data[0])); time.sleep(.03); self.resource.release()

class MonitorRW:
    def __init__(self):
        self.cv=threading.Condition(); self.readers=0; self.writer=False; self.waiting_writers=0
    def read(self,n,data,log):
        with self.cv:
            while self.writer or self.waiting_writers: self.cv.wait()
            self.readers+=1
        log.append((n,"READ",data[0])); time.sleep(.02)
        with self.cv:
            self.readers-=1
            if self.readers==0: self.cv.notify_all()
    def write(self,n,data,log):
        with self.cv:
            self.waiting_writers+=1
            while self.writer or self.readers: self.cv.wait()
            self.waiting_writers-=1; self.writer=True
        data[0]+=1; log.append((n,"WRITE",data[0])); time.sleep(.03)
        with self.cv: self.writer=False; self.cv.notify_all()

class Dekker:
    def __init__(self): self.flag=[False,False]; self.turn=0
    def enter(self,i):
        j=1-i; self.flag[i]=True
        while self.flag[j]:
            if self.turn==j:
                self.flag[i]=False
                while self.turn==j: time.sleep(0)
                self.flag[i]=True
    def leave(self,i): self.turn=1-i; self.flag[i]=False

class Peterson:
    def __init__(self): self.flag=[False,False]; self.turn=0
    def enter(self,i):
        j=1-i; self.flag[i]=True; self.turn=j
        while self.flag[j] and self.turn==j: time.sleep(0)
    def leave(self,i): self.flag[i]=False

def two_party_demo(lock_type):
    lock=lock_type(); data=[0]; log=[]; guard=threading.Lock()
    def participant(i,kind):
        for _ in range(3):
            lock.enter(i)
            if kind=="WRITE": data[0]+=1
            with guard: log.append((kind,data[0]))
            time.sleep(.01); lock.leave(i)
    a=threading.Thread(target=participant,args=(0,"READ"))
    b=threading.Thread(target=participant,args=(1,"WRITE"))
    a.start(); b.start(); a.join(); b.join()
    return len(log),data[0]

def run_rw(cls):
    obj=cls(); data=[0]; log=[]; gate=threading.Condition(); turn=[0]
    jobs=[("R1","read"),("W1","write"),("R2","read"),("W2","write"),("R3","read")]
    def worker(k,n,kind):
        with gate:
            while turn[0]!=k: gate.wait()
            turn[0]+=1; gate.notify_all()
        getattr(obj,kind)(n,data,log)
    ts=[threading.Thread(target=worker,args=(i,n,k)) for i,(n,k) in enumerate(jobs)]
    for t in ts:t.start()
    for t in ts:t.join()
    return log,data[0]

def main():
    rows=[]
    for name,typ in [("Dekker (2 participants)",Dekker),("Peterson (2 participants)",Peterson)]:
        count,val=two_party_demo(typ); rows.append([name,"two-party demo",count,val])
        print(name,":",count,"operations; final value",val)
    for name,typ in [("Semaphore",SemaphoreRW),("Monitor",MonitorRW)]:
        log,val=run_rw(typ); rows.append([name,"multiple readers/writers",len(log),val])
        print("\\n",name,"events:",log,"final value:",val)
    Path("results").mkdir(exist_ok=True)
    with open("results/summary.csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["method","scope","operations","final_shared_value"]); w.writerows(rows)
if __name__=="__main__": main()
