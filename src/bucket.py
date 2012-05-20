"""
- Module: It contains the bucket data structure with all the operations implemented as different functions
- CSE690 Concurrent and Distributed Algorithms
- Nitin Aggarwal (SBU ID: 108266663)
- Kademlia - A peer-to-peer network DHT
"""

# Import the required modules
from node import Node
from constants import BUCKET_SIZE,HASH_SIZE
from hash import computeIntHash

class Bucket:
    """
    It represents the bucket, which contains peers information,
    and is part of the Routing table.
    """
    def __init__(self, minValue, maxValue):
        """ 
        it represents the constructor for the class,
        which initializes the object variables  
        @param minValue: the starting value for the range of the bucket
        @type minValue: integer which varies from 0 to 2**HASH_SIZE
        @param maxValue: the end value for the range of the bucket
        @type maxValue: integer which varies from 0 to 2**HASH_SIZE    
        """  
        self.peerList = list()
        self.minValue = minValue
        self.maxValue = maxValue
        
    def __len__(self):
        """ 
        returns the length of the peers list in the bucket  
        """  
        return len(self.peerList)
    
    def hashInRange(self, hashValue):
        """ 
        checks whether the provided hashValue is within range of the bucket
        @param hashValue: 160-bit identifier computed through sha
        @type hashValue: string or integer representation
        @return: boolean value true or false
        """  
        if isinstance(hashValue, str):
            hashValue = computeIntHash(hashValue)
        return self.minValue <= hashValue < self.maxValue
       
    def getPeer(self, peerId):
        """
        returns the peer object for the corresponding peer id
        @param peerId: 160-bit identifier computed through sha
        @type peerId: string or integer representation
        @return: Node object from the bucket's peerList
        """
        index = self.peerList.index(peerId)
        return self.peerList[index]
    
    def addPeer(self, peer):
        """
        add the peer object to the bucket. 
        Different Scenarios:
            - If already in the list, move to the end
            - If not in the list, and list has space, add it to the end
        @param peer: peer to be added to the bucket's peerList
        @type peer: Kademlia.node.Node
        @raise Exception: if bucket is full
        """
        # Case 1
        if peer in self.peerList:
            self.peerList.remove(peer)
            self.peerList.append(peer)
        # Case 2
        elif len(self.peerList) < BUCKET_SIZE:
            self.peerList.append(peer)
        # Case 3
        else:
            raise Exception("No space in bucket to insert new peer")
    
    def getPeers(self, count=-1,smallest=True):
        """
        return list of peers containing requested number of peers
            1. Default: 
              - count value -1 signifies return all peers
              - smallest value true signifies that smallest k elements have to be returned
            2. Scenarios
              - Requested number of peers is less than the peers in the list 
                 - Send the requested number from the beginning of the list
              - Requested number of peers is more than the peers in the list
                 - Send all the peers in the list
        @param count: number of peers to be returned from the bucket's peerList
        @type count: integer, default to -1, which signifies return all
        @param smallest: signifies whether the peers to be returned should be with id's smallest or largest
        @type smallest: boolean, default to True, which signifies return the smallest Id's peers
        @return: list of peers i.e. Node object
        """
        # Default case 1
        if count <= 0:
            count = len(self.peerList)

        # Get the number of peers in the bucket
        length = len(self.peerList)
        listPeers = self.peerList
        
        # Scenario 1
        if count < length:
            if(smallest == True):
                listPeers = listPeers[0:count]
            else:
                listPeers = listPeers[-count:]
        # Scenario 2
        # No modifications to the list required 
        
        return listPeers

    def removePeer(self, peer):
        """
        remove the node from the bucket's peerList 
        @param peer: node object to be removed from the bucket
        @type peer: Kademlia.node.Node
        """
        self.peerList.remove(peer)
    
    def printBucket(self):
        """
        print all the node information from the bucket's peerList,
        along with the range information of the bucket 
        """
        print("Bucket: MinValue: "+str(self.minValue)+" MaxValue: "+str(self.maxValue))
        for peer in self.peerList:
            print(peer.processId)
        
### Test Scenarios ###
import unittest

class TestBucket(unittest.TestCase):
    """
    It represents the test class to test the bucket module
    for different functionalities
    """
    def test_add(self):
        """
        tests bucket module for addition operations
        """
        node1 = Node(33)
        node2 = Node(85)
        bucket = Bucket(0, 2**HASH_SIZE)
        bucket.addPeer(node1)
        for i in range(5):
            bucket.addPeer(Node(i))
        bucket.addPeer(node1)
        bucket.addPeer(node2)
        
        
    def test_get(self):
        """
        tests bucket module for get and print operations
        """
        node = Node(32)
        bucket = Bucket(0, 2**HASH_SIZE)
        bucket.addPeer(node)
        for i in range(5):
            bucket.addPeer(Node(i))
        nodes = bucket.getPeers(3)
        for peer in nodes:
            print(peer.processId)
        print("bucket")
        bucket.printBucket()