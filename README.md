# MESSAGE QUEUING USING NATIVE POSTGRESQL

Implementing in python of https://www.crunchydata.com/blog/message-queuing-using-native-postgresql  
Consumer parallel processing.   
main.py contains an example to implement the queue

## Requirements
So what makes up a minimal queuing solution? Effectively, we need the following:
  
* a table to hold events or items to be processed
* something to enqueue/put items in the table
* something to dequeue/consume these items
* do so without locking