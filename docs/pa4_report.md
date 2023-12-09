# Programming Assignment 4 Report
> **Name**:   Jiayin Fan<br>
> **Email**:      jfan22@bu.edu<br>
> **Student ID**: U80694562<br>

## Design Decisions
### 1. Histogram Buckets
The `IntHistogram` class optimizes memory usage by avoiding the storage of individual values. Instead, it aggregates data 
into fixed-width buckets and counts occurrences within each bucket. This design not only minimizes memory usage but also 
ensures constant space complexity. The use of buckets offers an effective trade-off 
between accuracy and efficiency, making it suitable for large datasets.

### 2. Dynamic Programming
The `JoinOptimizer` employs a dynamic programming strategy to get the most cost-effective join order in database queries. 
This method evaluates all possible join combinations, caching the optimal ones for each subset of joins. 
This approach ensures that the final join order is not just locally optimal for each subset but globally 
optimal across the entire query. The use of dynamic programming reduces computational overhead by reusing 
previously computed results, thereby enhancing the efficiency of the query optimization process.

## Challenges
Completing this assignment took me approximately 3 hours per day. The instructions for the histogram component of this 
assignment were quite clear and has minimal challenges. The development of the `JoinOptimizer` has a more complex challenge because 
it takes me quite a lot of time to understand the requirements and structures.