

## Data analysis

The table is analyzed for cost-based query optimization later on

Main collection:
- Max-min value
- number of distinct value
- number of identical value
- .....

## Multi-thread

Each SQL query uses one thread

## Optimizer

### SQL Rewrite

> Constant transfer
> 
> a.x=b.y & b.y < c  ==> a.x < c

> Remove filter
> 
> a.x < 100 & a.x < 200  ==> a.x < 100
> 
> There should be no more than two filters on each column
(equal,greater,less,(greater less))

> Always empty
> 
> a.x > 100 & b.x<10 ==> null
>

## Estimate cost

Record the cost of each filtered table. The cost of joining two tables is the minimum cost of two tables. The connection that costs less is executed first.



