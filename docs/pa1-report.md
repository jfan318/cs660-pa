## Programming Assignment 1 Report

### Design Decisions
#### Use of Iterators
I use the HeapPage iterator for sequentially scanning pages and files, as its functionality closely aligns with what
the SeqScan class requires. Most other iterators are implemented using basic logic or by adapting the provided sample iterator.

### Missing or Incomplete Elements
#### Error Handling
Error handling mechanisms should be incorporated for various methods. This includes scenarios such as attempting to access
a non-existent page or when the system encounters memory shortages.

#### Class Organization
The current implementation needs better organization. There are unused private members within the classes that can 
lead to confusion.

### Challenges
The assignment took me 4-5 hours/day to complete and was completed by myself.

One of the challenging part was grasping the structure and hierarchy of the database project, 
especially the interrelations among `DBfile`, `Page`, and `Table`. I found that implementing the HeapFile and its
iterator class consumed the most time. Initially, I overlooked the necessity to read the file during the constructor's 
creation and mistakenly believed the issues stemmed from the iterator's implementation.

Also, the presence of double inclusions and the absence of include guards resulted in redefinition errors. 
Resolving these issues required a lot amount of troubleshooting.
