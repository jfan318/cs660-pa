# Programming Assignment 2 Report
> **Name**:   Jiayin Fan<br>
> **Email**:      jfan22@bu.edu<br>
> **Student ID**: U80694562<br>

## Design Decisions
### 1. Page eviction policy
For the evictPage method, I use a FIFO (First-In-First-Out) policy. It is a basic approach which simply evicts pages in the order they were added to the buffer pool.

### 2. Use of Dynamic Casting:
Dynamic casting is employed to convert base class pointers to derived class pointers, preventing type mismatch errors.
This design choice adds a layer of safety and also enhances code readability.

### 3. Update of Parent Pointers:
Parent pointers are updated whenever there are changes in the internal structure of the B-Tree, such as during page splits 
or redistributions. This is a important step in maintaining the consistency and structure of the B-Tree.

## Missing or Incomplete Elements
### 1. Error Handling
Error handling mechanisms should be incorporated for various methods. I noticed that there were errors in several IF 
conditions, but I have not yet find a efficient method to fix them.

### 2. Class Organization
The current implementation needs better organization. There are unused parameters within several methods that should be 
cleaned.

### 3. Bonus parts
Methods for merging pages are not completed.

## Challenges
The assignment took me roughly 5 hours/day to complete and it was completed by myself.

A great amount of time was spent on understanding the provided B-Tree data structure, including all the members and methods. While the B-Tree algorithms and logic were taught in class, implementing them
still required a comprehensive understanding of the data structure and a meticulous approach to handle various cases and maintain
the properties of the B-Tree. One specific challenge was to maintain the correct redistribution of entries or tuples while 
updating parent entries and keeping the balance of the tree.

Also, like in assignment 1, the presence of double inclusions and the absence of include guards still resulted in redefinition errors.
With the experience from the previous assignment, resolving these issues did not cost a lot of time.