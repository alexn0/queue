## Implementations of local filesystem and in-memory message queues

### Description

#### Script to build the code:
```sh
./gradlew jar
```

####Architecture of the solution

The algorithm we use is a modification of Michael & Scott's lock-free queue algorithm.
Steps of the algorithm:

1) When we add element to the queue we choose the next "batch" element.

2) When we call commit method we remove the committed element from queue by changing links of previous and next element so that the previous element points to the next element and vice versa.

3) When we call fail method we consider 2 cases:

  a) The failed element is the head object of queue - in this case we change the head object to point to the previous element.

  b) Otherwise - we remove the element from queue by changing links of previous and next element and add it to the tail of the queue.

4) When we poll elements from queue we first check if there are some non-committed old elements to readd them to the queue as we do for failed elements. After that we change the head of the queue to point to the nearest batch element and return list of elements, the batch element and all elements that are followed by this batch element.

5) We reuse the above algorithm for local filesystem and memory based queue.

#### Disclaimer
1) We don't remove files from filesystem when we remove elements from the local filesystem queue.
2) If you run tests it will create a lot of files in temp directory.
3) There seem to be left some bugs, so it is not recommended to use it anyhow.