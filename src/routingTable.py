"""
- Module: It contains the routing table data structure with all the operations implemented as different functions
- CSE690 Concurrent and Distributed Algorithms
- Nitin Aggarwal (SBU ID: 108266663)
- Kademlia - A peer-to-peer network DHT
"""

# Import the required modules
from constants import HASH_SIZE,K, BUCKET_SIZE
from bucket import Bucket
from node import Node
from hash import computeIntHash
import random

class RoutingTable:
    """
    It represents the routing table for the node, 
    and contains peers information in different buckets
    """
    def __init__(self, node):
        """ 
        it represents the constructor for the class,
        which initializes the object variables  
        @param node: the node routing table belongs to
        @type node: Kademlia.node.Node
        """  
        self.node = node
        self.bucketList = [Bucket(0, pow(2,HASH_SIZE))]
        
    def bucketIndexForInt(self, nodeId):
        """ 
        returns the index of the bucket in the bucket list 
        that could contain the provided node id
            @param nodeId: node id for which bucket needs to be find 
            @type nodeId: integer representation of 160-bit identifier computed through sha
            @return: index of suitable bucket in the bucket List
        """  
        index = -1
        for bucket in self.bucketList:
            if nodeId >= bucket.minValue and nodeId <= bucket.maxValue:
                index = self.bucketList.index(bucket)
        return index
    
    #return     
    def findNodes(self, value):
        """ 
        returns K nodes from its routing table 
        closest to the provided node id
            @param value: node id for which K nearest nodes to find 
            @type value: Kademlia.node.Node or integer, string representation 
            of 160-bit identifier computed through sha
            @return: list of K nodes nearest to the provided id
            @raise Exception: if value provided is not of suitable type 
        """  
        #check if nodeId has a valid type
        if isinstance(value, str):
            num = computeIntHash(value)
        elif isinstance(value, Node):
            num = value.nodeId
        elif isinstance(value, int):
            num = value
        else:
            raise Exception("findNodes expects integer, string, or Node argument")
            
        nodes = []
        bucketIndex = self.bucketIndexForInt(num)
        nodes = nodes + self.bucketList[bucketIndex].peerList
        
        if len(nodes) < K:
            # need more nodes
            minValue = bucketIndex - 1
            maxValue = bucketIndex + 1
            while len(nodes) < K and (minValue >= 0 or maxValue < len(self.bucketList)):
                if minValue >= 0:
                    nodes = nodes + self.bucketList[minValue].getPeers(K - len(nodes), self.node.nodeId, True)
                    minValue = minValue - 1
                if maxValue < len(self.bucketList):
                    nodes = nodes + self.bucketList[maxValue].getPeers(K - len(nodes), self.node.nodeId, False)
                    maxValue = maxValue + 1
        nodes.sort()           
        return nodes[0:K]
        
    def splitBucket(self, bucket):
        """ 
        splits the bucket mid way when its size goes beyond the limit,
        thereby creating two buckets
            @param bucket: Bucket object of the bucket to be split 
            @type bucket: Kademlia.bucket.Bucket  
        """   
        bucket.peerList.sort()
        mid = bucket.peerList[int(BUCKET_SIZE/2)].nodeId
        newBucket = Bucket(mid, bucket.maxValue)
        bucket.maxValue = mid - 1
        self.bucketList.insert(self.bucketList.index(bucket) + 1, newBucket)
        
        # transfer nodes from bucket to newBucket
        for peer in bucket.peerList:
            if peer.nodeId > bucket.maxValue:
                newBucket.addPeer(peer)
                
        # remove the transferred nodes from the old bucket
        for peer in newBucket.peerList:
            bucket.removePeer(peer)
        
    def addPeer(self, peer):
        """
        add the peer object in the routing table 
        to the suitable bucket. 
        Different Scenarios:
            - If it is the node itself, that routing table belongs to,
              simply return
            - If while adding to the list, the exception is received because
              bucket is full, the buckets are split
        @param peer: peer to be added to the bucket's peerList
        @type peer: Kademlia.node.Node
        """
        assert peer.nodeId != None
        if peer.nodeId == self.node.nodeId: 
            return
        
        # get the bucket for this node
        bucketIndex = self. bucketIndexForInt(peer.nodeId)
        # check to see if node is in the bucket already
        try:
            self.bucketList[bucketIndex].addPeer(peer)
        except Exception:
            if(self.bucketList[bucketIndex].minValue <= peer.nodeId < self.bucketList[bucketIndex].maxValue):
                self.splitBucket(self.bucketList[bucketIndex])
                self.addPeer(peer)
            elif len(self.bucketList) >= HASH_SIZE:
                # our table is FULL, this is really unlikely
                print("Hash Table is FULL!  Increase K!")
                return
            else:
                print("Invalid peer id, not in hash range "+str(peer.nodeId))
                return
            
    def removePeer(self, peer):
        """
        remove the node from the routing Table
            @param peer: node id of object to be removed
            @type peer: integer representation of 160-bit identifier 
            computed through sha
        """
        assert peer.nodeId != None
        if peer.nodeId == self.node.nodeId: 
            return
        
        # get the bucket for this node
        bucketIndex = self. bucketIndexForInt(peer.nodeId)
        #print("delete: "+str(bucketIndex))
        # check to see if node is in the bucket already
        try:
            self.bucketList[bucketIndex].removePeer(peer)
        except Exception:
            #print("Exception - doesn't exist")
            pass       
   
    def replaceDeadNode(self, dead, new):
        """
        replace the dead node with the new node. It is generally required 
        when the routing table has reached its limit
        @param dead: node object to be removed
        @type dead: Kademlia.node.Node 
        @param new: node object to be added
        @type new: Kademlia.node.Node 
        """
        # Replace dead node with new node
        bucketIndex = self.bucketIndexForInt(dead.nodeId)
        try:
            peer = self.bucketList[bucketIndex].getPeer(dead)
        except ValueError:
            return
    
        self.bucketList[bucketIndex].removePeer(peer)
        if new:
            self.bucketList[bucketIndex].addPeer(new)
            
    def printBucketList(self):
        """
        print all the node information from the routing table,
        with bucket-wise separation 
        """
        print("BucketList")
        for bucket in self.bucketList:
            print("Bucket: MinValue: "+str(bucket.minValue)+" MaxValue: "+str(bucket.maxValue))
            print(str(bucket.peerList))
            
    def randomPeer(self):
        """
        returns randomly any peer from the routing table
        """
        buckets = len(self.bucketList)
        index = random.randint(0,buckets-1)
        sizeBucket = len(self.bucketList[index].peerList)
        element =  random.randint(0,sizeBucket - 1)
        return self.bucketList[index].peerList[element]   
            
### Test Scenarios ###
import unittest

class TestRoutingTable(unittest.TestCase):
    """
    It represents the test class to test the routingTable module
    for different functionalities
    """
    def test_add(self):
        """
        tests routing table module for add,delete nodes and print operations
        """
        node1 = Node(32)
        node2 = Node(185)
        routingTable = RoutingTable(node1)
        for i in range(20):
            routingTable.addPeer(Node(i))
        routingTable.addPeer(node2)
        routingTable.printBucketList()
        routingTable.removePeer(node2)
        routingTable.printBucketList()
        
        
    def test_find(self):
        """
        tests routing table module to test crucial find operations
        """
        node1 = Node(321)
        node2 = Node(1854)
        routingTable = RoutingTable(node1)
        for i in range(20):
            routingTable.addPeer(Node(i))
        nodes = routingTable.findNodes(node2.nodeId)
        print("Nearby nodes")
        for node in nodes:
            print(node.processId)
        peerNew = routingTable.findNodes(1172651830431226490231730313420679550754695126018)
        print("findNodes")
        for node in peerNew:
            print(node.processId)
        temp = routingTable.randomPeer()
        print("Random: "+str(temp.processId))