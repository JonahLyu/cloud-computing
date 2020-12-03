import time, kazoo, logging
from kazoo.client import KazooClient
import logging

ELECTION_PATH = '/master'

class Election:

    def __init__(self, zk, electionPath, myPath):
        self.electionPath = electionPath
        self.zk = zk
        self.isMaster = False
        self.myID = myPath.split("/")[2] #/master/id_123
        self.myPath = myPath
        self.masterPath = None
        self.killNow =  False
        logging.info("My id is:  %s" % self.myID)

    def kill_myself(self,signum, frame):
        self.zk.delete(self.myPath)
        if(self.isMaster) :
            self.zk.delete(self.masterPath)
        self.killNow = True	
		 
    def isMasterServer(self):
	    return self.isMaster	
		
    def on_node_delete(self, event) :
        #in case of deletion start elections(perform a vote)
        if event.type == kazoo.protocol.states.EventType.DELETED:
            logging.info('Master just died, new master election start...')
            self.ballot(self.zk.get_children(self.electionPath))
            
	#perform a vote..	
    def ballot(self,children):
        new_master = min(children)  #choose the minimum one as master
        masterPath = self.electionPath + "/master_current"
        self.zk.ensure_path(masterPath) #creat masterPath znode if it doesn't exist
        self.masterPath = f'{masterPath}/{new_master}' #/master/master_current/id_123
        if(self.myID == new_master) :
            self.zk.create(self.masterPath, ephemeral=True) 
            self.isMaster = True
            logging.info ("Master is: %s" % (self.masterPath)) 
            logging.info ("I am master now") 
            return True
        else:
            self.zk.exists(self.masterPath, self.on_node_delete) #watch the master delete event
            self.isMaster = False
            logging.info ("Master is: %s" % (self.masterPath)) 
            logging.info ("I am worker now") 
            return False


                    
if __name__ == '__main__':
    zkhost = "127.0.0.1:2181" #default ZK host
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
    zk = KazooClient(zkhost) 
    zk.start()

    zk.ensure_path(ELECTION_PATH)
    masterPath = ELECTION_PATH + "/id_"
    myPath = zk.create(masterPath, ephemeral=True, sequence=True)
    election = Election(zk, ELECTION_PATH, myPath)

    election.ballot(zk.get_children(ELECTION_PATH))

    while (election.killNow == False) :
        time.sleep(1)
    logging.info("I was killed gracefully")
