"""
- Module: It contains all the hash related functionalities required for Kademlia Implementation
- CSE690 Concurrent and Distributed Algorithms
- Nitin Aggarwal (SBU ID: 108266663)
- Kademlia - A peer-to-peer network DHT
"""

from hashlib import sha1
import random
   
def generateRandom(length):
    """
    it generates a random string of characters of a particular length
    @param length:  length of the character string to be generated
    @type length: integer
    @return: utf-8 format randomly generated string of characters 
    """
    randStr = ''
    i = 0
    while i < length:
        randStr += chr(random.randint(0,255))
        i = i + 1
    return randStr.encode(encoding='utf_8', errors='strict')

def computeIntHash(strHash):
    """
    it generates an integer representation for the 20 byte string of characters
    @param strHash:  character string which needs to be converted into integer
    @type strHash: string
    @return: integer representation for string of characters 
    """
    assert len(strHash) == 20
    return int.from_bytes(bytes(strHash),'big')

def computeStringHash(intHash):
    """
    it generates a string representation for the integer hash
    @param intHash:  integer which needs to be converted into string of byte characters
    @type intHash: integer
    @return: byte string representation for the integer hash 
    """
    strValue = hex(intHash)[2:]
    if len(strValue) % 2 != 0:
        strValue = '0' + strValue
    strValue = strValue.decode('hex')
    return (20 - len(strValue)) *'\x00' + strValue
    
def distance(a, b):
    """
    computes distance between 160-bit hash values expressed as 20-character strings
    @param a:  string of byte characters
    @type a: string
    @param b:  string of byte characters
    @type b: string
    @return: distance in integer representation between two strings 
    """
    return computeIntHash(a) ^ computeIntHash(b)

def newID():
    """
    returns a new pseudo-random globally unique ID string generated through sha
    @return: byte string representation
    """
    h = sha1()
    h.update(generateRandom(20))
    return h.digest()

def newIDInRange(minValue, maxValue):
    """
    method to generate a within range randomId
    @param minValue: the starting value for the range
    @type minValue: integer which varies from 0 to 2**HASH_SIZE
    @param maxValue: the end value for the range
    @type maxValue: integer which varies from 0 to 2**HASH_SIZE    
    @return: byte string representation of an id within the range 
    """
    return minValue + computeIntHash(newID()) % (maxValue - minValue)
    
### Test Scenarios ###
import unittest

class TestHash(unittest.TestCase):
    """
    It represents the test class to test the hash module
    for testing generated random id's and on distance metric
    """
    test = [
            ((sha1("foo".encode(encoding='utf_8', errors='ignore')).digest(), sha1("foo".encode(encoding='utf_8', errors='ignore')).digest()), 0),
            ((sha1("bar".encode(encoding='utf_8', errors='ignore')).digest(), sha1("bar".encode(encoding='utf_8', errors='ignore')).digest()), 0)
            ]
    def test_length(self):
        """
        tests the module for generating id's and of requested length
        """
        self.assertEqual(len(newID()), 20)
        print(computeIntHash(newID()))
        print(computeIntHash(newID()))
    def test_randomRange(self):
        """
        tests the module for generating random id's within range
        """
        a = computeIntHash(newID())
        b = computeIntHash(newID())
        c = newIDInRange(a, b)
        assert(a <= c <= b)
    def testDist(self):
        """
        tests the module for distance metric already known
        """
        for pair, dist in self.test:
            self.assertEqual(distance(pair[0], pair[1]), dist)
    def testCommutitive(self):
        """
        tests the module for commutative property of distances
        """
        for i in range(100):
            x, y, z = newID(), newID(), newID()
            self.assertEqual(distance(x,y) ^ distance(y, z), distance(x, z))

if __name__ == '__main__':
    unittest.main()   
