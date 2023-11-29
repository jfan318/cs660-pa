# Programming Assignment 3 Report
> **Name**:   Jiayin Fan<br>
> **Email**:      jfan22@bu.edu<br>
> **Student ID**: U80694562<br>

## Design Decisions
### 1. Switch Statement
In the methods of `Join` and `IntegerAggregatorIterator`, I used the switch statement. This approach was chosen because 
it has better readability and organization when dealing with more than three conditions, as compared to the traditional
if-else statement. It simplifies the logic, making the code more maintainable and understandable.

### 2. Helper Class/Methods
I created a helper class for `IntegerAggregator` to store data relevant to all five SQL aggregates (COUNT, SUM, AVG, MIN, 
MAX). This class controls the process of accessing and manipulating aggregated data, providing a more efficient approach 
than managing multiple individual variables. It serves as a improtant point for handling aggregation-related operations, 
increasing the efficiency and readability of the code.


## Missing or Incomplete Elements
### 1. Error Handling
The current implementation lacks error handling, especially where the input includes various types of data. While the 
framework for type checking and exception handling is outlined, it is not fully developed. 

### 2. HashEquiJoin not Completed
I was unable to complete the HashEquiJoin method. This was mainly due to my own time management challenges, which
makes me fail to write custom tests for this method. 



## Challenges
The assignment took me roughly 5-6 hours/day to complete.

Completing this assignment took me approximately 5-6 hours per day. The assignment introduced
new features and incorporated a wider variety of data types, which required additional learning and adaptation. A significant
challenge was maintaining the consistency and accuracy of various related variables throughout the execution process. Additionally, 
ensuring the persistence of some variables beyond the scope of their methods was complex task, where even a minor 
misplacement could result in loss of data.