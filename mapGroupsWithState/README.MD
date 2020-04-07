# Stateful grouping

**mapGroupsWithState** method allows aggregate records group into a single records.
Aggregation function operates on next record and previously aggregated value.

```scala
ds.groupByKey(...)
  .mapGroupsWithState(someTimeout)(aggFun)
```

**flatMapGroupsWithState** method allows aggregate records group into a set of records.

```scala
ds.groupByKey(...)
  .flatMapGroupsWithState(OutputMode.Update(), someTimeout)(aggFun)
```