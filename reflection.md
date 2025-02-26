
### Overall Design

- I implemented table cardinality via the method mentioned in the write up: overriding the getRowCount method for a custom statistic
- I retreived statistics and table content directly from DuckDb using the JDBC. Rather than making JDBC scan work with Enumerable convention, I just used the JDBC to get the results and then converted them to a list of rows inside my custom ScannableTable.



### Obstacles and solutions
- Rel2Sql doesn't [handle correlated subqueries very well.](https://issues.apache.org/jira/browse/CALCITE-5420). To address this, I used RelDecorrelator to decorrelate the query. There were two options for when to do this: before optimization or after. Ultimately, I found that decorrelating after optimization resulted in better SQL plans to run in DuckDb, but that decorrelating before optimization resulted in better performance when running through RelRunner. 
This is an example of the error that occurs:

```
SELECT * FROM (SELECT * FROM l1.lineitem) AS l1 JOIN (SELECT * FROM l2.nation) AS n ON l1.n_nationkey = n.n_nationkey;
```

- Rel2Sql also doesn't like converting EnumerableConvention to SQL. I first had to convert from EnumerableConvention to LogicalConvention using ToLogicalConverter. 
- RelRunner actually optimizes the query again, so if we run the optimized node through it, we get errors that say insufficent rule set. This might be because a RelNode is already in EnumerableConvention by the time we try to optimize it a second time, so all of the CoreRules that expect LogicalConvention no longer can apply. I also found that I had to reset the planner between optimization calls, otherwise leftover state from the first optimization would affect the second (the one I was using within RelRunner).
- EnumerableAggregateRule can only convert specific logical aggregate functions to enumerable aggregate functions. To address this, I had to use the AggregateReduceFunctionsRule to convert the aggregate functions to a format that EnumerableAggregateRule could understand.
- Limit sort doesn't work. Why?



### Tuning rules set

After I had constructed a system that could optimize queries, convert back to SQL, and also run in RelRunner, I started tuning the rules set.

An additional optimization I made at this point was to change to the volcano top-down planner to top-down optimization. For q2 that changed my cost from 10^33 to 10^22 rows.

After my initial set of rules, I began to examine the plans I was outputting to determine what rules would produce the transformations I wanted. Broadly, my approach was to identify which optimized plans contained nested loop joins, and try to find rules that would allow them to be transformed into hash joins. Essentially, this meant moving predicates close enough to the join that the predicate and the NLJ could be combined into a hash join.

Broadly, these fit into two categories:
- Join rules. I added rules that would both reocrder joins based on communative and associative properties, and also push filters through joins. I did find that some rules, like JoinPushThroughJoinRule, would explode the search space and cause a time out. I ommitted these rules. 
- Filter and project rules. I added rules that would either combine multiple filters into one, and also push filters and projectspast joins and past each other.

I fond that the ProjectToSemiJoinRule was useful for in-memory execution, but lengthened execution time in DuckDb. So I only use this rule to optimize for RelRunner. 


### Project Feedback

What was good:


What could be improved: 



















