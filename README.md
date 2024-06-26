# Query Optimizer Competition

<p>The point of this project was to improve the basic query optimizer we were given from a competition in CS 186 in order to reduce the IOs on a set of workloads.</p>
The improvements I have added are:

- Implemented LIFO and MRU buffer replacement policies
- Implemented INLJ, LeapfrogJoin, Sort-Merge Join, Grace Hash Join
- Implemented LeapfrogTriejoin which allows joins on multiple columns
- Duplicate record support in LeapfrogJoin and LeapfrogTriejoin (original paper didn't address it)
- Duplicate key support in the B+ Tree Index Structure

Inspiration for LFTJ and LFJ: https://arxiv.org/abs/1210.0481
