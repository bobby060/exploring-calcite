

https://issues.apache.org/jira/browse/CALCITE-5420

https://www.jofre.de/?p=1459


Limit doesn't work. LImit_Sort causes a random error too

- RelRunner runs optimization itself, so if we run the optimized node through it, we get an error.
- Average doesn't work...
- Changing to volcano top-down is super important - for q2 that switched my cost from 10^33 to 10^22 rows.

- Initially 22 and 16 ran veeery slowly in calcite. Working on fixing