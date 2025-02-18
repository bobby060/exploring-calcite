

https://issues.apache.org/jira/browse/CALCITE-5420

https://www.jofre.de/?p=1459


Limit doesn't work. LImit_Sort causes a random error too

### Initial implementation


- RelRunner runs optimization itself, so if we run the optimized node through it, we get an error.
- Average doesn't work...
- Changing to volcano top-down is super important - for q2 that switched my cost from 10^33 to 10^22 rows.

- Initially 22 and 16 ran veeery slowly in calcite. Working on fixing



### Tuning rules set

- When I decorrelate a RelNode that isn't in Logical convention, the decorrelation introduces some errors where a table alias will be referenced within the subquery. Example:

```
SELECT * FROM (SELECT * FROM l1.lineitem) AS l1 JOIN (SELECT * FROM l2.nation) AS n ON l1.n_nationkey = n.n_nationkey;
```

This is a problem because the table alias l1 is not valid within the subquery.



