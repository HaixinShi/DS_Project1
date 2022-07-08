# CS422 Project 1 - Relational Operators and Execution Models - Report

## Task 1: Implement a volcano-style tuple-at-a-time engine (30%)

we need to implement six basic operators (**Scan**, **Filter**, **Project**, **Join**, **Aggregate** and **Sort**) in a volcano-style tuple-at-a-time operators.

### Scan
* **open():** Initialize index at the beginning.
* **next():** Every time, increase the index and call `row_store.getRow` according to the index. When index reaches to the size of the row store, return NilTuple to notify its caller.
* **close():** Empty.

### Filter
We can implement it in recursion tuple by tuple, as well as while-loop selecting the entire input. Here, we adopt the second method.
* **open():** Open the underlying operator and collect inputs from it as a list and initializa an index.
* **next():** Every time, increase the index and get a tuple from the input list according to the index. Then, apply `predicate` on the tuple, if it is true, then return the tuple to the caller; otherwise, redo above steps until index reaching the size of the input list. When index reaches to the size of the list, return NilTuple to notify its caller.
* **close():** Close the underlying operator.

### Project
We can implement it like `Filter`, but we can also implement it tuple by tuple: every time, call `next()` of the underlying operator to get a tuple; then we apply 'evaluator' on this tuple to remove some fields and then return it to the caller.

### Join
Actually, we can only collect all the tuples of left operator to build a map. Then at `next()`, we recursively collect valid tuples from right operator by calling `next()`. However, we can also collect all the inputs and make the result list at a time:
* **open():** Open the underlying operators and collect left inputs as a list and right inputs as a list. First iterate the left list and build a map where for a specific key, we may have a list of tuples. Second, iterate the right list and merge the items of it with the items of left list according to the map. From these two steps, we can collect a result list. Also we initialize an index.
* **next():** Every time, increase the index and get a tuple from the result list according to the index, returning it to the caller. When index reaches to the size of the list, return NilTuple to notify its caller.
* **close():** Close the underlying operator.

### Sort
The main task for this operator is to build a `comparator` function. We collect the fields that need sorting and make every related values of tuples be comparable object, so as to compare tuples.
* **open():** Open the underlying operator and collect inputs from it as a list and sort it with the `comparator` we build. Besides, we need to be careful about that `offset` means the starting position to get tuples, and `fetch` means the number of tuples we need to fetch. So we initialize an index by taking the above two values into account.
* **next():** Every time, increase the index and get a tuple from the sorted list according to the index, returning it to the caller. When index reaches to the limited size, return NilTuple to notify its caller.
* **close():** Close the underlying operator.

### Aggregate
* **open():** Open the underlying operator and collect inputs from it as a list. According to `groupSet`, we divide the tuples into different groups. Then we iterate the groups, during which we perform all the aggregates inside `aggCalls` to the tuples of each group. Every group would have an aggregate result and we merge them as the result list.
* **next():** Every time, increase the index and get a tuple from the result list according to the index, returning it to the caller. When index reaches to the limited size, return NilTuple to notify its caller.
* **close():** Close the underlying operator.

## Task 2: Late Materialization (naive) (20%)

In this task, we need to implement late materialization.

### Stitch
Here I tried to finish everything by collecting all the inputs from left operator and right operator, but it would lead to timeout problem. Therefore, I tried to only collect the inputs from one operator and solved this problem.
* **open():** We collect the inputs from left operator and build a map. `Key` is the vid of LateTuple and `Value` is the value of LateTuple.
* **next():** Every time, get a tuple from the right operator, and then try to figure out whether its vid is contained in the map: if yes, retrn it to the caller; otherwise, recursively call `next()` until we get NilLateTuple.
* **close():** Close the underlying operator.

### Drop
We simply return `tuple.get.value` at `next()` function.

### LateFilter & LateProject 
It is very similar to corresponding operators in Task 1. The only difference is that we need to retrieve `Tuple`(value) from the `LateTuple`. After finish operators with `Tuple`, we reconstruct the `LateTuple` to return to the caller. 

### LateJoin
It is very similar to `Join` in Task 1, but we need to build the key of a LateTuple in the map with the `vid` here.

## Task 3: Query Optimization Rules (20%)

In this task, we mainly replace some logical operators based on `Stitch` with more efficient operators based on `Fetch`.

### Fetch
We mainly implement `next()` function:
* **next():** Every time, get a LateTuple from the input operator, and then try to figure out its vid is available in the column store: if yes, get the element; otherwise, recursively call `next()` until we get NilLateTuple. After getting an element, we merge it with the input LateTuple. What is more, if we have projects for the column store, we would need to apply it to the element before merging it with the input LateTuple.

### LazyFetchRule
This rule would match three logical operators: LogicalStitch, the left input logical operator of it, and the right input logical operator which is the LateColumnScan. Therefore, we create a LogicalFecth with the left input logical operator of LogicalStitch and the LateColumnScan and return it directly so as to replace LogicalStitch.

### LazyFetchProjectRule
This rule would match four logical operators: LogicalStitch, the left input logical operator of it, the right input logical operator which is the LogicalProject, and LateColumnScan. LateColumnScan would be the input logical operator for LogicalProject. 

In order to replace LogicalStitch and LogicalProject, we create LogicalFecth with the left input logical operator of LogicalStitch and the LateColumnScan, as well as `getProjects` from LogicalProject. In Fetch operator, these projects would perform on the column store so it would have the same effect of Project operator.

### LazyFetchFilterRule

This rule would match four logical operators: LogicalStitch, the left input logical operator of it, the right input logical operator which is the LogicalFilter, and LateColumnScan. LateColumnScan would be the input logical operator of LogicalFilter. 

According to the assertion, we need to let the LogicalFilter be the root and its input logical operator be LogicalFetch. This LogicalFetch would replace LogicalStitch, the original root, as mentioned before. It is like `floating up` the LogicalFilter.

Since the index of the condition of LogicalFilter is based on the record type of LateColumnScan, we need to adjust it to the record type of input operator LogicalFetch, so as to correctly filter tuples on conditions.


## Task 4: Execution Models (30%)

This tasks focuses on the column-at-a-time execution model, building gradually from an operator-at-a-time execution over
columnar data. The Filter, Scan, Project should not prune tuples!

### Subtask 4.A: Enable selection-vectors in operator-at-a-time execution

This task is rather simple since it can reuse the code from Task 1. The only thing we need to do is to change the input format and the output format:
* **input format:** We apply `val tuples = input.execute().transpose` so as to change the column store into row store. We only consider active tuples in Join and Aggregate while we would not prune tuples in Filter, Scan, Project. Besides, we use `map` method to remove the flag coumn for further processing.
* **output format:** For Aggregate and Join, we apply `results.transpose :+ (0 until results.transpose.head.size).map(_ => true)` so as to change the row store into column store while appending the flag column with all `true`. For Filter we would append new flag column. For Project, we would append original flag column.

### Subtask 4.B: Column-at-a-time with selection vectors and mapping functions

This task is also rather simple since it can reuse the code from Task 1. The main thing we need to do is to change the input format and the output format:
* **input format:** We apply `val tuples = input.execute().transpose` so as to change the column store into row store. We only consider active tuples in Join and Aggregate while we would not prune tuples in Filter, Scan, Project. Besides, we use `map` method to remove the flag coumn for further processing.
* **output format:** For Aggregate and Join, we apply `results.transpose.map(i => toHomogeneousColumn(i)) :+ toHomogeneousColumn((0 until results.transpose.head.size).toArray.map(_ => true))` so as to change the row store into column store while appending the flag column with all `true`. For Filter we would append new flag column. For Project, we would append original flag column.
* **Filter:** `mappredicate` would input pure columns without flag coumn and output an array of flags to indicate which tuple is active.
* **Project:** `evals` has a list of project expressions. We should apply each of them to the pure data without flag column. Finally, we append the original flag column since projection would not affect activeness of tuples.