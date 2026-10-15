"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file:
  1. Configuration and MongoDB client
  2. Time / rounding helpers (business rules R1 - R5, R8)
  3. Serialisation (BSON <-> API: datetimes become epoch milliseconds)
  4. Request models and shared validators
  5. Query / pipeline builders (shared by the endpoints and /admin/explain)
  6. Endpoints
"""
import calendar
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # never overrides variables already set in the environment

# tz_aware=True: PyMongo returns UTC-aware datetimes instead of naive ones, so no
# value is ever silently interpreted in the server's local time zone.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

GRACE_SECONDS = 10 * 60          # R2
MIN_OVERTIME_MINUTES = 30        # R3
HALF_DAY_BELOW_HOURS = Decimal("4.50")  # R5
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
StatusT = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceT = Literal["PRESENT", "WFH", "ON_DUTY"]

DATE_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
HHMM_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"

INDEXES = {
    "employees": [
        ([("emp_code", ASCENDING)], {"name": "uniq_emp_code", "unique": True}),
        ([("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "dept_emp_code"}),
        ([("department", ASCENDING), ("joined_on", ASCENDING)], {"name": "dept_joined_on"}),
        ([("joined_on", ASCENDING)], {"name": "joined_on"}),
    ],
    "attendance_logs": [
        ([("emp_code", ASCENDING), ("date", ASCENDING)], {"name": "uniq_emp_code_date", "unique": True}),
        ([("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "date_desc_emp_code"}),
        ([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "status_date_emp_code"}),
        ([("emp_code", ASCENDING), ("punch_in", DESCENDING)], {"name": "emp_code_punch_in"}),
    ],
}


def ensure_indexes() -> None:
    """create_index is idempotent: an index that already exists with the same spec is a no-op."""
    for coll, specs in INDEXES.items():
        for keys, opts in specs:
            try:
                db[coll].create_index(keys, **opts)
            except PyMongoError as exc:  # keep starting; /health reports the database state
                log.error("could not create index %s on %s: %s", opts["name"], coll, exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time and rounding helpers
# --------------------------------------------------------------------------- #
def now_utc() -> datetime:
    return datetime.now(UTC)


def from_ms(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: BSON dates are always UTC
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def truncate_seconds(dt: datetime) -> datetime:
    """R1: instants are truncated to whole seconds before any calculation."""
    return dt.replace(microsecond=0)


def truncate_millis(dt: datetime) -> datetime:
    """BSON dates only hold milliseconds; trim so what we return equals what is stored."""
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)


def parse_hhmm(value: str) -> time:
    h, m = map(int, value.split(":"))
    return time(h, m)


def is_overnight(emp: dict) -> bool:
    return parse_hhmm(emp["shift_end"]) <= parse_hhmm(emp["shift_start"])


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: the IST calendar date of the punch-in; for an overnight shift a punch-in before
    shift_end belongs to the shift that started the previous day."""
    local = punch_in.astimezone(IST)
    if is_overnight(emp) and local.time() < parse_hhmm(emp["shift_end"]):
        return local.date() - timedelta(days=1)
    return local.date()


def shift_start_at(att_date: date, emp: dict) -> datetime:
    return datetime.combine(att_date, parse_hhmm(emp["shift_start"]), tzinfo=IST)


def shift_end_at(att_date: date, emp: dict) -> datetime:
    end = datetime.combine(att_date, parse_hhmm(emp["shift_end"]), tzinfo=IST)
    return end + timedelta(days=1) if is_overnight(emp) else end


def compute_late_minutes(punch_in: datetime, att_date: date, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then floor minutes since shift start."""
    seconds = int((punch_in - shift_start_at(att_date, emp)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, rounded 2 decimals half-up (Decimal, not float round())."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out: datetime, att_date: date, emp: dict) -> int:
    """R3: floor minutes after shift end (next day for overnight shifts), only if >= 30."""
    minutes = int((punch_out - shift_end_at(att_date, emp)).total_seconds()) // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime],
                   att_date: date, emp: dict) -> dict:
    """The four derived values for a record, per the table in DATA_MODEL.md section 3."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {
        "work_hours": None,
        "late_minutes": compute_late_minutes(punch_in, att_date, emp),
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        out["work_hours"] = compute_work_hours(punch_in, punch_out)
        out["overtime_minutes"] = compute_overtime(punch_out, att_date, emp)
        out["half_day"] = Decimal(str(out["work_hours"])) < HALF_DAY_BELOW_HOURS  # R5
    return out


def half_up(value: Any, places: int) -> Optional[float]:
    """R8 rounding. MongoDB's $round is round-half-to-even, so final rounding happens here."""
    if value is None:
        return None
    if isinstance(value, Decimal128):
        value = value.to_decimal()
    dec = value if isinstance(value, Decimal) else Decimal(str(value))
    return float(dec.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = map(int, month.split("-"))
    last = calendar.monthrange(year, mon)[1]
    return f"{month}-01", f"{month}-{last:02d}"


def count_working_days(start: date, end: date) -> int:
    """R7: Mon - Fri between start and end inclusive (no holiday calendar)."""
    if start > end:
        return 0
    days = (end - start).days + 1
    full_weeks, rest = divmod(days, 7)
    extra = sum(1 for i in range(rest) if (start + timedelta(days=i)).weekday() < 5)
    return full_weeks * 5 + extra


# --------------------------------------------------------------------------- #
# 3. Serialisation
# --------------------------------------------------------------------------- #
def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": to_ms(doc.get("created_at")),
    }


def _instant_or_value(value: Any) -> Any:
    return to_ms(value) if isinstance(value, datetime) else value


def history_out(entry: dict) -> dict:
    return {
        "at": to_ms(entry.get("at")),
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": {
            field: {"from": _instant_or_value(c.get("from")), "to": _instant_or_value(c.get("to"))}
            for field, c in (entry.get("changes") or {}).items()
        },
    }


def record_out(doc: dict) -> dict:
    """Missing late/overtime mean 0, missing half_day means false, missing history means []."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": to_ms(doc.get("punch_in")),
        "punch_out": to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day") or False),
        "history": [history_out(h) for h in doc.get("history") or []],
    }


# --------------------------------------------------------------------------- #
# 4. Request models and validators
# --------------------------------------------------------------------------- #
# strict=True rejects floats, strings and booleans; the bounds reject seconds-looking values.
EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]


def invalid(loc: list, msg: str, value: Any = None):
    """Raise a 422 with FastAPI's default validation body ({"detail": [...]})."""
    raise RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": value}])


def parse_date(value: str, loc: list) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        invalid(loc, "must be a valid date YYYY-MM-DD", value)


def require(value: Any, loc: list) -> Any:
    if value is None:
        invalid(loc, "field required")
    return value


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=HHMM_RE)
    shift_end: str = Field("18:30", pattern=HHMM_RE)
    joined_on: str = Field(pattern=DATE_RE)

    @field_validator("joined_on")
    @classmethod
    def _real_date(cls, v: str) -> str:
        date.fromisoformat(v)  # ValueError -> 422 for e.g. 2026-02-30
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceT = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    # Derived fields are not declared, so a client sending them is ignored.
    status: Optional[StatusT] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} can not be null; omit it to leave it unchanged")
        return self


Page = Annotated[int, Query(ge=1)]
PageSize = Annotated[int, Query(ge=1, le=100)]
Month = Annotated[str, Query(pattern=MONTH_RE)]
OptMonth = Annotated[Optional[str], Query(pattern=MONTH_RE)]
Limit = Annotated[int, Query(ge=1, le=50)]


# --------------------------------------------------------------------------- #
# 5. Query / pipeline builders - one place, used by the endpoint and by /admin/explain
# --------------------------------------------------------------------------- #
WEEKDAY_EXPR = {  # Mon-Fri check on a "YYYY-MM-DD" string; isoDayOfWeek: 1 = Mon ... 7 = Sun
    "$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}, 5]
}


def is_presence(field: str) -> dict:
    return {"$in": [field, PRESENCE]}


def present_value(prefix: str = "$") -> dict:
    """R5/R6 credit for one record: 1, 0.5 for a half day, 0 for a non-presence status."""
    return {
        "$cond": [
            is_presence(f"{prefix}status"),
            {"$cond": [{"$eq": [{"$ifNull": [f"{prefix}half_day", False]}, True]}, 0.5, 1]},
            0,
        ]
    }


def attendance_list_query(emp_code, date_from, date_to, status, page, page_size) -> dict:
    q: dict = {}
    if emp_code:
        q["emp_code"] = emp_code
    if date_from or date_to:
        d_from = parse_date(date_from, ["query", "date_from"]) if date_from else None
        d_to = parse_date(date_to, ["query", "date_to"]) if date_to else None
        if d_from and d_to and d_from > d_to:
            invalid(["query", "date_from"], "date_from must be on or before date_to")
        q["date"] = {}
        if d_from:
            q["date"]["$gte"] = d_from.isoformat()
        if d_to:
            q["date"]["$lte"] = d_to.isoformat()
    if status:
        q["status"] = status
    return {
        "filter": q,
        "sort": [("date", DESCENDING), ("emp_code", ASCENDING)],
        "skip": (page - 1) * page_size,
        "limit": page_size,
    }


def employee_monthly_pipeline(emp_code: str, month: str) -> list:
    first, last = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first, "$lte": last}}},
        {"$group": {
            "_id": None,
            # R7: only Mon-Fri records count towards present_days.
            "present_days": {"$sum": {"$cond": [WEEKDAY_EXPR, present_value(), 0]}},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}},
            "total_late_minutes": {"$sum": {"$max": [{"$ifNull": ["$late_minutes", 0]}, 0]}},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    match: dict = {"joined_on": {"$lte": last}}  # R9: joined on or before the end of the month
    if department:
        match["department"] = department
    log_weekday = {
        "$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$log.date", "format": "%Y-%m-%d",
                                                         "onNull": None}}}, 5]
    }
    has_hours = {"$and": [is_presence("$log.status"), {"$isNumber": "$log.work_hours"}]}
    late = {"$ifNull": ["$log.late_minutes", 0]}
    return [
        # Drive from employees so people with zero logs are still counted, and logs of
        # unknown employees are naturally ignored.
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": first, "$lte": last}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1, "work_hours": 1}},
            ],
            "as": "logs",
        }},
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},  # keep employees with no logs
        {"$set": {"log": "$logs"}},
        {"$group": {
            "_id": "$department",
            "employees": {"$addToSet": "$emp_code"},
            "present_days": {"$sum": {"$cond": [
                {"$and": [{"$ne": [{"$type": "$log"}, "missing"]}, log_weekday]}, present_value("$log."), 0]}},
            # Exact decimal sum so the average is not polluted by float error before R8 rounding.
            "work_hours_sum": {"$sum": {"$cond": [has_hours, {"$toDecimal": "$log.work_hours"}, 0]}},
            "work_hours_n": {"$sum": {"$cond": [has_hours, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [late, 0]}, 1, 0]}},
            "total_late_minutes": {"$sum": {"$cond": [{"$gt": [late, 0]}, late, 0]}},
            "leave_count": {"$sum": {"$cond": [{"$eq": ["$log.status", "LEAVE"]}, 1, 0]}},
            "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$log.status", "ON_DUTY"]}, 1, 0]}},
        }},
        {"$project": {
            "_id": 0,
            "department": "$_id",
            "headcount": {"$size": "$employees"},
            "present_days": 1,
            "avg_work_hours": {"$cond": [{"$gt": ["$work_hours_n", 0]},
                                         {"$divide": ["$work_hours_sum", "$work_hours_n"]}, None]},
            "late_count": 1,
            "total_late_minutes": 1,
            "leave_count": 1,
            "on_duty_count": 1,
        }},
        {"$sort": {"department": 1}},
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first, "$lte": last}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {
            "from": "employees",
            "localField": "_id",
            "foreignField": "emp_code",
            "pipeline": [{"$project": {"_id": 0, "name": 1, "department": 1}}],
            "as": "emp",
        }},
        {"$unwind": "$emp"},  # drops logs whose employee does not exist - before ranking
    ]
    if department:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department
    pipeline += [
        # $rank is standard competition ranking: ties share a rank, the next rank is skipped (1, 2, 2, 4).
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking, so ties at the cutoff are kept
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0,
            "rank": 1,
            "emp_code": "$_id",
            "name": "$emp.name",
            "department": "$emp.department",
            "total_late_minutes": 1,
            "late_count": 1,
        }},
    ]
    return pipeline


def department_trend_pipeline(department: str, d_from: date, d_to: date) -> list:
    start, end = d_from.isoformat(), d_to.isoformat()
    day = {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}
    rate = {"$divide": [{"$toDecimal": "$present_count"}, {"$toDecimal": "$headcount"}]}
    rate_half_up_4 = {"$divide": [{"$floor": {"$add": [{"$multiply": [rate, 10000]}, Decimal128("0.5")]}}, 10000]}
    return [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": start, "$lte": end}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
            ],
            "as": "logs",
        }},
        # Turn every employee into events: one "join" event (clamped into the range, so a running
        # sum gives R9 headcount per day) plus one event per log.
        {"$project": {"_id": 0, "events": {"$concatArrays": [
            [{
                "date": {"$min": [{"$max": ["$joined_on", start]}, end]},
                "joins": {"$cond": [{"$lte": ["$joined_on", end]}, 1, 0]},
                "present": 0,
                "late": 0,
            }],
            {"$map": {"input": "$logs", "as": "l", "in": {
                "date": "$$l.date",
                "joins": 0,
                "present": present_value("$$l."),
                "late": {"$cond": [{"$gt": [{"$ifNull": ["$$l.late_minutes", 0]}, 0]}, 1, 0]},
            }}},
        ]}}},
        {"$unwind": "$events"},
        {"$group": {
            "_id": "$events.date",
            "joins": {"$sum": "$events.joins"},
            "present_count": {"$sum": "$events.present"},
            "late_count": {"$sum": "$events.late"},
        }},
        {"$set": {"day": day}},
        # Gap filling: exactly one row per calendar day (upper bound is exclusive).
        {"$densify": {"field": "day", "range": {
            "step": 1, "unit": "day",
            "bounds": [datetime.combine(d_from, time(), UTC), datetime.combine(d_to + timedelta(days=1), time(), UTC)],
        }}},
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "headcount": {"$sum": "$joins", "window": {"documents": ["unbounded", "current"]}},
        }}},
        {"$set": {
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
        }},
        {"$set": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]}, rate_half_up_4, None]}}},
        # $avg ignores nulls and returns null when the window has no numbers; the window only
        # sees rows inside the requested range, so the first rows use a shorter window.
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "moving_avg_7d": {"$avg": "$attendance_rate", "window": {"documents": [-6, "current"]}},
        }}},
        {"$sort": {"day": 1}},
        {"$project": {"_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "present_count": 1,
                      "late_count": 1, "attendance_rate": 1, "moving_avg_7d": 1}},
    ]


def trend_range(d_from: Optional[str], d_to: Optional[str]) -> tuple[date, date]:
    start = parse_date(require(d_from, ["query", "from"]), ["query", "from"])
    end = parse_date(require(d_to, ["query", "to"]), ["query", "to"])
    if end < start:
        invalid(["query", "to"], "to must be on or after from")
    if (end - start).days + 1 > MAX_TREND_DAYS:
        invalid(["query", "to"], f"range can not exceed {MAX_TREND_DAYS} days")
    return start, end


# --------------------------------------------------------------------------- #
# 6. Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = truncate_millis(now_utc())
    try:
        db.employees.insert_one(doc)  # the unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: Page = 1, page_size: PageSize = 20):
    q = {"department": department} if department else {}
    total = db.employees.count_documents(q)
    cursor = db.employees.find(q).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


def get_employee(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ts = truncate_seconds(from_ms(body.punched_at) if body.punched_at is not None else now_utc())
    att_date = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": att_date.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, att_date, emp),
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date): exactly one concurrent insert wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    ts = truncate_seconds(from_ms(body.punched_at) if body.punched_at is not None else now_utc())
    record = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$ne": None, "$lte": ts}},
        sort=[("punch_in", DESCENDING)],
    )
    if record is None:
        raise HTTPException(404, "no punch-in found")
    if record.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    if ts <= record["punch_in"]:
        invalid(["body", "punched_at"], "punch-out must be after punch-in", body.punched_at)
    if ts - record["punch_in"] > MAX_SHIFT:
        invalid(["body", "punched_at"], "punch-out must be within 24 hours of punch-in", body.punched_at)

    att_date = date.fromisoformat(record["date"])
    derived = derived_fields(record["status"], record["punch_in"], ts, att_date, emp)
    late = record.get("late_minutes") or 0  # late was fixed at punch-in; keep the stored value
    updated = db.attendance_logs.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},  # only one concurrent punch-out can match
        {"$set": {"punch_out": ts, "work_hours": derived["work_hours"], "late_minutes": late,
                  "overtime_minutes": derived["overtime_minutes"], "half_day": derived["half_day"]}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[StatusT] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    spec = attendance_list_query(emp_code, date_from, date_to, status, page, page_size)
    total = db.attendance_logs.count_documents(spec["filter"])
    cursor = db.attendance_logs.find(spec["filter"]).sort(spec["sort"]).skip(spec["skip"]).limit(spec["limit"])
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(body: RegularizeIn, emp_code: str, date_str: Annotated[str, Path(alias="date")]):
    att_date = parse_date(date_str, ["path", "date"])
    emp = get_employee(emp_code)
    record = db.attendance_logs.find_one({"emp_code": emp_code, "date": att_date.isoformat()})
    if record is None:
        raise HTTPException(404, "no attendance record for that date")

    old = {
        "status": record["status"],
        "punch_in": record.get("punch_in"),
        "punch_out": record.get("punch_out"),
        "work_hours": record.get("work_hours"),
        "late_minutes": record.get("late_minutes") or 0,
        "overtime_minutes": record.get("overtime_minutes") or 0,
        "half_day": bool(record.get("half_day") or False),
    }
    new_status = body.status or old["status"]
    new_in = truncate_seconds(from_ms(body.punch_in)) if body.punch_in is not None else old["punch_in"]
    new_out = truncate_seconds(from_ms(body.punch_out)) if body.punch_out is not None else old["punch_out"]

    if new_status not in PRESENCE:
        if body.punch_in is not None or body.punch_out is not None:
            invalid(["body"], f"punch times can not be set with status {new_status}")
        new_in = new_out = None  # ABSENT / LEAVE clear both punch times
    else:
        if new_in is None:
            invalid(["body", "punch_in"], "a presence status requires a punch_in")
        if attendance_date(new_in, emp) != att_date:
            invalid(["body", "punch_in"], "punch_in must stay on the record's attendance date", body.punch_in)
        if new_out is not None and not (new_in < new_out <= new_in + MAX_SHIFT):
            invalid(["body", "punch_out"], "punch_out must be after punch_in and within 24 hours", body.punch_out)

    if (new_status, new_in, new_out) == (old["status"], old["punch_in"], old["punch_out"]):
        invalid(["body"], "the request changes nothing")

    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
           **derived_fields(new_status, new_in, new_out, att_date, emp)}
    changes = {k: {"from": old[k], "to": new[k]} for k in old if old[k] != new[k]}
    entry = {"at": truncate_millis(now_utc()), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: only apply if nobody changed the record (or its history) since we read it.
    # A concurrent editor gets 409 instead of silently losing an audit entry.
    updated = db.attendance_logs.find_one_and_update(
        {
            "_id": record["_id"],
            "status": old["status"],
            "punch_in": old["punch_in"],
            "punch_out": old["punch_out"],
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(record.get("history") or [])]},
        },
        {"$set": new, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "the record was modified concurrently; retry")
    return record_out(updated)


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: Month):
    emp = get_employee(emp_code)
    first, last = month_bounds(month)
    working_days = count_working_days(
        max(date.fromisoformat(first), date.fromisoformat(emp["joined_on"])), date.fromisoformat(last)
    )
    rows = list(db.attendance_logs.aggregate(employee_monthly_pipeline(emp_code, month)))
    agg = rows[0] if rows else {}
    present = agg.get("present_days", 0)
    pct = None
    if working_days:
        pct = half_up(Decimal(str(present)) * 100 / Decimal(working_days), 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": half_up(present, 2),
        "leave_days": agg.get("leave_days", 0),
        "late_count": agg.get("late_count", 0),
        "total_late_minutes": agg.get("total_late_minutes", 0),
        "total_overtime_minutes": agg.get("total_overtime_minutes", 0),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: Month, department: Optional[str] = None):
    items = list(db.employees.aggregate(department_summary_pipeline(month, department)))
    for item in items:
        item["present_days"] = half_up(item["present_days"], 2)
        item["avg_work_hours"] = half_up(item["avg_work_hours"], 2)
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: Month, limit: Limit = 10, department: Optional[str] = None):
    items = list(db.attendance_logs.aggregate(late_leaderboard_pipeline(month, limit, department)))
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    d_from: Annotated[Optional[str], Query(alias="from")] = None,
    d_to: Annotated[Optional[str], Query(alias="to")] = None,
):
    start, end = trend_range(d_from, d_to)
    if db.employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    items = list(db.employees.aggregate(department_trend_pipeline(department, start, end)))
    for item in items:
        item["present_count"] = half_up(item["present_count"], 2)
        item["attendance_rate"] = half_up(item["attendance_rate"], 4)
        item["moving_avg_7d"] = half_up(item["moving_avg_7d"], 4)
    return {"department": department, "items": items}


ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: OptMonth = None,
    department: Optional[str] = None,
    limit: Limit = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[StatusT] = None,
    d_from: Annotated[Optional[str], Query(alias="from")] = None,
    d_to: Annotated[Optional[str], Query(alias="to")] = None,
    page: Page = 1,
    page_size: PageSize = 20,
):
    if endpoint == "attendance_list":
        coll = "attendance_logs"
        spec = attendance_list_query(emp_code, date_from, date_to, status, page, page_size)
        cmd = {"find": coll, "filter": spec["filter"], "sort": dict(spec["sort"]),
               "skip": spec["skip"], "limit": spec["limit"]}
    else:
        if endpoint == "employee_monthly":
            coll = "attendance_logs"
            pipeline = employee_monthly_pipeline(require(emp_code, ["query", "emp_code"]),
                                                 require(month, ["query", "month"]))
        elif endpoint == "department_summary":
            coll = "employees"
            pipeline = department_summary_pipeline(require(month, ["query", "month"]), department)
        elif endpoint == "late_leaderboard":
            coll = "attendance_logs"
            pipeline = late_leaderboard_pipeline(require(month, ["query", "month"]), limit, department)
        else:
            coll = "employees"
            start, end = trend_range(d_from, d_to)
            pipeline = department_trend_pipeline(require(department, ["query", "department"]), start, end)
        cmd = {"aggregate": coll, "pipeline": pipeline, "cursor": {}}
    raw = db.command("explain", cmd, verbosity="executionStats")
    return {"endpoint": endpoint, "collection": coll, "explain": json.loads(json_util.dumps(raw))}
