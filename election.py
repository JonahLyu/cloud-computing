import time, sys, kazoo, logging
from kazoo.client import KazooClient
from kazoo.client import KazooState

MASTER_PATH = '/master'

class Election:

    def __init__(self, zk, election_path, self_path):
        self.election_path = election_path
        self.zk = zk
        self.is_leader = False
        self.child_id = self_path.split("/")[1]
        self.path = self_path
        self.leader_path = None
        self.kill_now =  False

    def kill_myself(self,signum, frame):
        self.zk.delete(self.path)
        if(self.is_leader) :
            self.zk.delete(self.leader_path)
        self.kill_now = True	
		 
    def is_leading(self):
	    return self.is_leader	
		

    def on_node_delete(self, event) :
        #in case of deletion start elections(perform a vote)
        if event.type == kazoo.protocol.states.EventType.DELETED:
            print('Master died')
            self.ballot(self.zk.get_children(self.election_path))
            
	#perform a vote..	
    def ballot(self,children):
        next_master = min(children)  #choose the minimum one as master
        master_path = self.election_path + "master_current/"
        self.zk.ensure_path(master_path) #creat master_path znode if it doesn't exist
        self.leader_path = master_path + next_master #/master/master_current/id_123
        if(self.child_id == next_master) :
            self.zk.create(self.leader_path, ephemeral=True) 
            self.is_leader = True
            return True
        else:
            print ("Master is = %s " %(self.leader_path)) 
            self.zk.exists(self.leader_path, self.on_node_delete) #watch the master delete event
            self.is_leader = False
            return False


                    
if __name__ == '__main__':
    zkhost = "127.0.0.1:2181" #default ZK host
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
    zk = KazooClient(zkhost) 
    zk.start()
   
    master_path = MASTER_PATH + "/id_"
    child = zk.create(master_path, ephemeral=True, sequence=True)
    election = Election(zk, MASTER_PATH, child)

    if election.ballot(zk.get_children(MASTER_PATH)) == False :
	    print("I'm a worker")
    else :
        print("I'm the leader now")

    while (election.kill_now == False) :
        time.sleep(1)
    print("I was killed gracefully")
