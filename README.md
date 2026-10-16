# Employee Attendance & Analytics API

FastAPI + MongoDB service for punch-in/punch-out, regularization with an audit trail, and attendance analytics built on
MongoDB aggregation pipelines. All application code is in `app/main.py`.

## Run

```bash
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017"           # or put MONGO_URI / MONGO_DB in a local .env
export MONGO_DB="attendance_db"
uvicorn app.main:app --port 8000
```

Requires MongoDB 6.0+ (uses `$densify`, `$setWindowFields` and `$lookup` with `localField` + `pipeline`).
Indexes are created idempotently at startup. Real environment variables take priority over `.env`.

## Notes

- Instants are epoch milliseconds in the API and UTC BSON dates in MongoDB; the client is `tz_aware`.
- Final R8 rounding (half-up) is applied in Python after the aggregation, because MongoDB's `$round` rounds half to even.
  The trend's `attendance_rate` is rounded half-up inside the pipeline (with Decimal128) so the moving average is
  computed over the reported rates.
- "Range exceeds 92 days" for the trend is read as more than 92 calendar days inclusive.
- A PATCH counts as "changes nothing" when status, punch_in and punch_out are all unchanged.
- Concurrent PATCHes use optimistic concurrency (the update only matches if the record and its history length are
  unchanged since it was read); the loser gets 409.
- Tested against MongoDB 7.0 with the sample data, a randomized dataset checked against a plain-Python reference
  implementation (ties, no-log employees, weekends, mid-month joiners, overnight shifts), parallel-request race tests,
  and a 100k-record dataset where every `/admin/explain/*` plan uses `IXSCAN` with no `COLLSCAN`.
  
