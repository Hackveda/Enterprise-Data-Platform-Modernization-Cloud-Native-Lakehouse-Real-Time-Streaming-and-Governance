from __future__ import annotations

import json
import re
import sqlite3
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

DEFAULT_DB = Path("data/mdm.sqlite")


def _norm_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _norm_email(value: str | None) -> str:
    return _norm_text(value)


def _norm_phone(value: str | None) -> str:
    digits = re.sub(r"\D", "", value or "")
    return digits[-10:] if len(digits) >= 10 else digits


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class MasterResult:
    action: str
    master_customer_id: str
    duplicate: bool
    match_type: str
    match_score: float
    changed_fields: list[str]
    golden_record: dict[str, Any]


class CustomerMDM:
    """Small MDM reference service for golden-record creation and identity resolution.

    Matching order:
      1. deterministic email match
      2. deterministic phone match
      3. fuzzy name + country match

    The same master_customer_id is reused for duplicates and updates. Every incoming
    source record is retained in mdm_source_xref for lineage/audit purposes.
    """

    def __init__(self, db_path: str | Path = DEFAULT_DB):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS mdm_customer_golden (
                    master_customer_id TEXT PRIMARY KEY,
                    full_name TEXT NOT NULL,
                    email TEXT,
                    phone TEXT,
                    country TEXT,
                    address TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS ux_mdm_email
                    ON mdm_customer_golden(lower(email))
                    WHERE email IS NOT NULL AND email <> '';

                CREATE INDEX IF NOT EXISTS ix_mdm_phone
                    ON mdm_customer_golden(phone);

                CREATE TABLE IF NOT EXISTS mdm_source_xref (
                    source_system TEXT NOT NULL,
                    source_customer_id TEXT NOT NULL,
                    master_customer_id TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    PRIMARY KEY (source_system, source_customer_id)
                );

                CREATE TABLE IF NOT EXISTS mdm_change_audit (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    master_customer_id TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    changed_at TEXT NOT NULL,
                    source_system TEXT NOT NULL
                );
                """
            )

    def _candidate_rows(self) -> list[sqlite3.Row]:
        with self._connect() as con:
            return con.execute("SELECT * FROM mdm_customer_golden").fetchall()

    def _match(self, record: dict[str, Any]) -> tuple[sqlite3.Row | None, str, float]:
        email = _norm_email(record.get("email"))
        phone = _norm_phone(record.get("phone"))
        name = _norm_text(record.get("full_name"))
        country = _norm_text(record.get("country"))

        rows = self._candidate_rows()
        if email:
            for row in rows:
                if _norm_email(row["email"]) == email:
                    return row, "email_exact", 1.0
        if phone:
            for row in rows:
                if _norm_phone(row["phone"]) == phone:
                    return row, "phone_exact", 1.0

        best: tuple[sqlite3.Row | None, float] = (None, 0.0)
        if name:
            for row in rows:
                if country and _norm_text(row["country"]) != country:
                    continue
                score = SequenceMatcher(None, name, _norm_text(row["full_name"])).ratio()
                if score > best[1]:
                    best = (row, score)
        if best[0] is not None and best[1] >= 0.90:
            return best[0], "name_country_fuzzy", best[1]
        return None, "new_identity", 0.0

    def upsert(self, record: dict[str, Any]) -> MasterResult:
        required = ["source_system", "source_customer_id", "full_name"]
        missing = [key for key in required if not str(record.get(key, "")).strip()]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

        source_system = str(record["source_system"]).strip()
        source_customer_id = str(record["source_customer_id"]).strip()
        normalized = {
            "full_name": str(record["full_name"]).strip(),
            "email": _norm_email(record.get("email")) or None,
            "phone": _norm_phone(record.get("phone")) or None,
            "country": str(record.get("country") or "").strip().upper() or None,
            "address": str(record.get("address") or "").strip() or None,
        }

        with self._connect() as con:
            xref = con.execute(
                "SELECT master_customer_id FROM mdm_source_xref WHERE source_system=? AND source_customer_id=?",
                [source_system, source_customer_id],
            ).fetchone()

        if xref:
            with self._connect() as con:
                matched = con.execute(
                    "SELECT * FROM mdm_customer_golden WHERE master_customer_id=?",
                    [xref["master_customer_id"]],
                ).fetchone()
            match_type, match_score = "source_xref", 1.0
        else:
            matched, match_type, match_score = self._match(normalized)

        now = _now()
        changed_fields: list[str] = []

        if matched is None:
            master_customer_id = f"MC-{uuid.uuid4().hex[:12].upper()}"
            with self._connect() as con:
                con.execute(
                    """INSERT INTO mdm_customer_golden
                    (master_customer_id, full_name, email, phone, country, address, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    [master_customer_id, normalized["full_name"], normalized["email"], normalized["phone"], normalized["country"], normalized["address"], now, now],
                )
            action, duplicate = "created", False
        else:
            master_customer_id = matched["master_customer_id"]
            updates: dict[str, Any] = {}
            for field in ["full_name", "email", "phone", "country", "address"]:
                new_value = normalized[field]
                old_value = matched[field]
                if new_value not in (None, "") and new_value != old_value:
                    updates[field] = new_value
                    changed_fields.append(field)
            if updates:
                with self._connect() as con:
                    for field, new_value in updates.items():
                        con.execute(
                            "INSERT INTO mdm_change_audit(master_customer_id, field_name, old_value, new_value, changed_at, source_system) VALUES (?, ?, ?, ?, ?, ?)",
                            [master_customer_id, field, matched[field], new_value, now, source_system],
                        )
                    set_sql = ", ".join(f"{field}=?" for field in updates)
                    con.execute(
                        f"UPDATE mdm_customer_golden SET {set_sql}, updated_at=? WHERE master_customer_id=?",
                        [*updates.values(), now, master_customer_id],
                    )
                action = "updated"
            else:
                action = "duplicate"
            duplicate = True

        with self._connect() as con:
            con.execute(
                """INSERT INTO mdm_source_xref(source_system, source_customer_id, master_customer_id, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_system, source_customer_id) DO UPDATE SET
                    master_customer_id=excluded.master_customer_id,
                    last_seen_at=excluded.last_seen_at""",
                [source_system, source_customer_id, master_customer_id, now, now],
            )
            golden = dict(con.execute(
                "SELECT * FROM mdm_customer_golden WHERE master_customer_id=?",
                [master_customer_id],
            ).fetchone())

        return MasterResult(
            action=action,
            master_customer_id=master_customer_id,
            duplicate=duplicate,
            match_type=match_type,
            match_score=round(match_score, 4),
            changed_fields=changed_fields,
            golden_record=golden,
        )


def main() -> None:
    payload = json.load(sys.stdin)
    result = CustomerMDM().upsert(payload)
    print(json.dumps(asdict(result), indent=2))


if __name__ == "__main__":
    main()
