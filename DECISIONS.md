# DECISIONS.md

1. **Indexes.** `employees`: unique `emp_code` (duplicate-safe create, lookups); `(department, emp_code)` for the sorted
   department list; `(department, joined_on)` and `(joined_on)` for headcount in the summary and trend.
   `attendance_logs`: unique `(emp_code, date)` (the "one record per day" guarantee, monthly summary, the `$lookup`s);
   `(date desc, emp_code)` matches the list sort and the month range of the leaderboard; `(status, date desc, emp_code)`
   for status filters; `(emp_code, punch_in desc)` for "latest punch-in" at punch-out. I rejected an index on
   `late_minutes`: few queries filter on it alone and the date range is already selective.

2. **Punch-in race.** Both requests compute the same `(emp_code, date)` and call `insert_one`. There is no read first.
   MongoDB's unique index lets exactly one insert succeed; the other gets `DuplicateKeyError`, which I turn into 409.

3. **Ties.** `$setWindowFields` with `$rank` gives competition ranks (1, 2, 2, 4). I filter `rank <= limit` afterwards,
   so everyone tied at the cutoff is returned, sorted by `emp_code`, even if that is more than `limit` rows.

4. **Headcount.** The pipeline starts from `employees` (`joined_on <= month end`), not from logs, and `$lookup`s the
   month's logs. `$unwind` with `preserveNullAndEmptyArrays` keeps people with no logs, and headcount is the size of
   the set of `emp_code`s.

5. **At 100x.** Pre-aggregate daily per-department counters on write (or with a scheduled job), so analytics read a
   few rows per day instead of scanning millions of logs.
