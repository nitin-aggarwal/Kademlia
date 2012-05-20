"""
- Module: It contains the node data structure with all the operations implemented as different functions
- CSE690 Concurrent and Distributed Algorithms
- Nitin Aggarwal (SBU ID: 108266663)
- Kademlia - A peer-to-peer network DHT
"""

# Import the required modules
from hash import newID,computeIntHash

class Node:
    """
    It is the backbone class for storing the node information
    """
    def __init__(self, processId):
        """
        it represents the constructor for the class,
        which initializes the object variables  
        @param processId: the system assigned process Id
        @type processId: integer 
        """
        self.processId = processId
        self.nodeId = computeIntHash(newID())
    
    def __lt__(self, a):
        """
        overridden for comparison operations
        """
        if isinstance(a, Node):
            a = a.nodeId
        return self.nodeId < a
    def __le__(self, a):
        """
        overridden for comparison operations
        """
        if isinstance(a, Node):
            a = a.nodeId
        return self.nodeId <= a
    def __gt__(self, a):
        """
        overridden for comparison operations
        """
        if isinstance(a, Node):
            a = a.nodeId
        return self.nodeId > a
    def __ge__(self, a):
        """
        overridden for comparison operations
        """
        if isinstance(a, Node):
            a = a.nodeId
        return self.nodeId >= a
    def __eq__(self, a):
        """
        overridden for comparison operations
        """
        if isinstance(a, Node):
            a = a.nodeId
        return self.nodeId == a
    def __ne__(self, a):
        """
        overridden for comparison operations
        """
        if isinstance(a, Node):
            a = a.nodeId
        return self.nodeId != a
    
    def __hash__(self):
        """
        overridden hash function to use set for Node Objects
        """
        return self.nodeId
    def __repr__(self):
        """
        overridden repr function to print process id for the Node Objects
        """
        return str([self.processId])

## Test Scenarios
import unittest

class TestNode(unittest.TestCase):
    """
    It represents the test class to test the Node module
    for different functionalities
    """
    def testEquality(self):
        """
        tests Node module for equality operations
        """
        node1 = Node(23)
        node2 = Node(32)
        node2.nodeId = node1.nodeId + 1
        if node1 == node2:
            print("Equal")
        else:
            print("Not equal")
            
    def testListEquality(self):
        """
        tests node module for list equality operations
        """
        node1 = Node(23)
        node2 = Node(32)
        list1 = []
        list2 = []
        list1.append(node1)
        list2.append(node2)
        list1.append(node2)
        list2.append(node1)
        #node2.nodeId = node1.nodeId + 1
        if list1.sort() == list2.sort():
            print("Lists Equal")
        else:
            print("Lists Not equal")
        print(str(list1))
        # can compare equal length lists
        if[x for x in list1 if x not in list2]:
            print("Lists unequal")
            
if __name__ == "__main__":
    unittest.main()