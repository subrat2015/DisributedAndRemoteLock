# Context
Build a poc for learning remote and distributed lock by using redis

# Approach

## Remote Lock

1. Spin up the single redis instance
2. Use redis command [ SET key value NX EX seconds ] for acquiring lock
3. Use lua script for releasing the lock

## Distributed lock

1. Spin up the 5 redis instance running on port (6379, 6380, 6381, 6381, 6383)
2. Use remote lock for acquiring lock on single instance
3. If thread is able to acquire lock on more than 2 instance that means lock is distributed lock is acquired

# Steps to run
1 - Go to dockor-compose.yml file and install the docker
plugin, once the plugin installs successfully, we can
start this file and ultimatelly all the redis instance 
spin up automatically.

or 
1. docker-compose up -d
2. run DisributedAndRemoteLockApplication.java

