"""
- Module: It contains all the constant values required for Kademlia Implementation
- CSE690 Concurrent and Distributed Algorithms
- Nitin Aggarwal (SBU ID: 108266663)
- Kademlia - A peer-to-peer network DHT
"""

HASH_SIZE = 160
"""
Size of the identifier user in DHT in bits
"""

K = 5
"""
parallelism - nodes contacted in case of key search
"""

BUCKET_SIZE = 15
"""
Size of the bucket - maximum peers whose information is stored in the bucket
"""

TIMEOUT = 1
"""
Wait time for response from other process
"""

SLEEP = 3
"""
Deliberate sleep time introduced for better results view
"""

STABLE = 30
"""
Interval at which routing tables are refreshed
"""

KVUPDATE = 40
"""
Interval at which key-value pairs are republished
"""