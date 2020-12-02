import time, kazoo, logging
from kazoo.client import KazooClient

ELECTION_PATH = '/master'

class Election:

    def __init__(self, zk, election_path, my_path):
        self.election_path = election_path
        self.zk = zk
        self.is_master = False
        self.my_id = my_path.split("/")[2] #/master/id_123
        self.my_path = my_path
        self.master_path = None
        self.kill_now =  False
        print("My id is:  %s" % self.my_id)

    def kill_myself(self,signum, frame):
        self.zk.delete(self.my_path)
        if(self.is_master) :
            self.zk.delete(self.master_path)
        self.kill_now = True	
		 
    def is_master_server(self):
	    return self.is_master	
		
    def on_node_delete(self, event) :
        #in case of deletion start elections(perform a vote)
        if event.type == kazoo.protocol.states.EventType.DELETED:
            print('Master just died, new master election start...')
            self.ballot(self.zk.get_children(self.election_path))
            
	#perform a vote..	
    def ballot(self,children):
        new_master = min(children)  #choose the minimum one as master
        master_path = self.election_path + "/master_current"
        self.zk.ensure_path(master_path) #creat master_path znode if it doesn't exist
        self.master_path = f'{master_path}/{new_master}' #/master/master_current/id_123
        if(self.my_id == new_master) :
            self.zk.create(self.master_path, ephemeral=True) 
            self.is_master = True
            print ("Master is: %s" % (self.master_path)) 
            print ("I am master now") 
            return True
        else:
            self.zk.exists(self.master_path, self.on_node_delete) #watch the master delete event
            self.is_master = False
            print ("Master is: %s" % (self.master_path)) 
            print ("I am worker now") 
            return False


                    
if __name__ == '__main__':
    zkhost = "127.0.0.1:2181" #default ZK host
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
    zk = KazooClient(zkhost) 
    zk.start()

    zk.ensure_path(ELECTION_PATH)
    master_path = ELECTION_PATH + "/id_"
    my_path = zk.create(master_path, ephemeral=True, sequence=True)
    election = Election(zk, ELECTION_PATH, my_path)

    election.ballot(zk.get_children(ELECTION_PATH))

    while (election.kill_now == False) :
        time.sleep(1)
    print("I was killed gracefully")
