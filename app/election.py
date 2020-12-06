import time, kazoo, logging
from kazoo.client import KazooClient
import logging, utils

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
		
    def onNodeDelete(self, event) :
        #in case of deletion start elections(perform a vote)
        if event.type == kazoo.protocol.states.EventType.DELETED:
            logging.info('Master just died, new master election start...')
            self.ballorMaster(self.zk.get_children(self.electionPath))
            
	# choose a master from all candidates, master return true
    def ballorMaster(self,children):
        newMaster = min(children)  #choose the minimum one as master (oldest)
        masterPath = self.electionPath + "/master_current"
        self.zk.ensure_path(masterPath) #creat masterPath znode if it doesn't exist
        self.masterPath = f'{masterPath}/{newMaster}' #/master/master_current/id_123
        if (self.myID == newMaster) :
            # this is a master, create a master path
            self.zk.create(self.masterPath, ephemeral=True) 
            self.isMaster = True
            logging.info ("Master is: %s" % (self.masterPath)) 
            logging.info ("I am master now") 
            return True
        else:
            # this is a master backup, watch the master delete event
            self.zk.exists(self.masterPath, self.onNodeDelete)
            self.isMaster = False
            logging.info ("Master is: %s" % (self.masterPath)) 
            logging.info ("I am master backup now") 
            return False


                    
if __name__ == '__main__':
    zk = utils.init('out') #get out cluster zk client

    zk.ensure_path(ELECTION_PATH)
    masterPath = ELECTION_PATH + "/id_"
    myPath = zk.create(masterPath, ephemeral=True, sequence=True)
    
    election = Election(zk, ELECTION_PATH, myPath)
    election.ballorMaster(zk.get_children(ELECTION_PATH))

    while (election.killNow == False) :
        time.sleep(1)
    logging.info("I was killed gracefully")
