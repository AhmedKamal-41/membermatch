#!/usr/bin/env python3
"""Generate synthetic member data sources for MemberMatch.

Produces three deterministic files simulating real-world member data a
payer might need to reconcile:

    data/sources/source_a_members.csv   (500 rows, legacy system)
    data/sources/source_b_members.json  (400 rows, partner feed)
    data/sources/source_c_members.csv   (350 rows, API export)

All randomness is seeded (random.seed(42), Faker.seed(42)), so reruns
produce byte-identical output. Tests assert exact row counts, so do not
alter the cohort sizes without updating downstream fixtures.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, time, timezone
from pathlib import Path

from faker import Faker

random.seed(42)
Faker.seed(42)
fake = Faker("en_US")

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "data" / "sources"

SOURCE_A_PLANS = ["PPO_GOLD", "HMO_SILVER", "EPO_BRONZE", "MEDICARE_A"]
SOURCE_B_PLANS = ["GOLD", "SILVER", "BRONZE"]
SOURCE_C_PLANS = ["Gold PPO", "Silver HMO", "Bronze EPO", "Medicare Part A"]

NICKNAMES = {
    "John": "Jon",
    "Jonathan": "Jon",
    "William": "Will",
    "Robert": "Bob",
    "James": "Jim",
    "Michael": "Mike",
    "Richard": "Rick",
    "Thomas": "Tom",
    "Elizabeth": "Liz",
    "Katherine": "Kate",
    "Margaret": "Meg",
    "Christopher": "Chris",
    "Joseph": "Joe",
    "Daniel": "Dan",
    "Andrew": "Andy",
    "Matthew": "Matt",
    "Nicholas": "Nick",
    "Alexander": "Alex",
    "Benjamin": "Ben",
    "Samuel": "Sam",
}


def gen_person() -> dict:
    return {
        "first": fake.first_name(),
        "last": fake.last_name(),
        "dob": fake.date_between(start_date=date(1940, 1, 1), end_date=date(2005, 12, 31)),
        "ssn": f"{random.randint(0, 9999):04d}",
        "zip": f"{random.randint(1, 99999):05d}",
    }


def vary_first_name(name: str) -> str:
    """Return a plausible variant of ``name`` — nickname, initial, or typo."""
    r = random.random()
    if r < 0.30 and name in NICKNAMES:
        return NICKNAMES[name]
    if r < 0.50:
        return f"{name[0]}."
    if r < 0.70 and len(name) >= 3:
        return name[:2] + name[1] + name[2:]
    return name


def build_source_a_row(idx: int, p: dict) -> dict:
    first = p["first"]
    last = p["last"]
    dob_out = p["dob"].isoformat()
    ssn = p["ssn"]

    if random.random() < 0.03:
        if random.random() < 0.5:
            first = " " + first
        else:
            last = last + " "

    if random.random() < 0.05:
        dob_out = p["dob"].strftime("%m/%d/%Y")

    if random.random() < 0.02:
        last = last.lower()

    if random.random() < 0.01:
        ssn = ""

    return {
        "member_id": f"MEMBER_A_{idx + 1:05d}",
        "first_name": first,
        "last_name": last,
        "date_of_birth": dob_out,
        "ssn_last4": ssn,
        "zip": p["zip"],
        "plan_code": random.choice(SOURCE_A_PLANS),
    }


def build_source_b_row(idx: int, p: dict, is_dup: bool) -> dict:
    if is_dup:
        first = vary_first_name(p["first"])
    else:
        first = p["first"]
    last = p["last"]

    if random.random() < 0.15:
        last = last.upper()

    zip_code = p["zip"]
    if random.random() < 0.30:
        zip_code = f"{zip_code}-{random.randint(1000, 9999)}"

    return {
        "id": f"MEMBER_B_{idx + 1:05d}",
        "firstName": first,
        "lastName": last,
        "dob": p["dob"].isoformat(),
        "ssnLast4": p["ssn"],
        "zipCode": zip_code,
        "plan": random.choice(SOURCE_B_PLANS),
    }


def build_source_c_row(idx: int, p: dict) -> dict:
    given = p["first"].upper()
    family = p["last"]

    if random.random() < 0.20:
        midnight_utc = datetime.combine(p["dob"], time(0, 0, 0), tzinfo=timezone.utc)
        birth_date = str(int(midnight_utc.timestamp()))
    else:
        birth_date = p["dob"].isoformat()

    if random.random() < 0.40:
        postal = str(int(p["zip"]))
    else:
        postal = p["zip"]

    return {
        "memberIdentifier": f"MEMBER_C_{idx + 1:05d}",
        "givenName": given,
        "familyName": family,
        "birthDate": birth_date,
        "taxIdLast4": p["ssn"],
        "postal_code": postal,
        "planName": random.choice(SOURCE_C_PLANS),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    persons_a = [gen_person() for _ in range(500)]

    dup_ix_b = random.sample(range(500), 250)
    dups_b = [(persons_a[i], True) for i in dup_ix_b]
    new_b_persons = [gen_person() for _ in range(150)]
    new_b = [(p, False) for p in new_b_persons]
    persons_b = dups_b + new_b

    pool_for_c = persons_a + new_b_persons
    dup_ix_c = random.sample(range(len(pool_for_c)), 150)
    dups_c = [pool_for_c[i] for i in dup_ix_c]
    new_c_persons = [gen_person() for _ in range(200)]
    persons_c = dups_c + new_c_persons

    source_a_rows = [build_source_a_row(i, p) for i, p in enumerate(persons_a)]
    source_b_rows = [
        build_source_b_row(i, p, is_dup) for i, (p, is_dup) in enumerate(persons_b)
    ]
    source_c_rows = [build_source_c_row(i, p) for i, p in enumerate(persons_c)]

    path_a = OUT_DIR / "source_a_members.csv"
    with path_a.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "member_id",
                "first_name",
                "last_name",
                "date_of_birth",
                "ssn_last4",
                "zip",
                "plan_code",
            ],
        )
        writer.writeheader()
        writer.writerows(source_a_rows)

    path_b = OUT_DIR / "source_b_members.json"
    with path_b.open("w") as f:
        json.dump(source_b_rows, f, indent=2)
        f.write("\n")

    path_c = OUT_DIR / "source_c_members.csv"
    with path_c.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "memberIdentifier",
                "givenName",
                "familyName",
                "birthDate",
                "taxIdLast4",
                "postal_code",
                "planName",
            ],
        )
        writer.writeheader()
        writer.writerows(source_c_rows)

    total = len(source_a_rows) + len(source_b_rows) + len(source_c_rows)
    print(f"Source A: {len(source_a_rows)} rows → {path_a.relative_to(REPO)}")
    print(f"Source B: {len(source_b_rows)} rows → {path_b.relative_to(REPO)}")
    print(f"Source C: {len(source_c_rows)} rows → {path_c.relative_to(REPO)}")
    print(f"Total: {total} rows across 3 sources")


if __name__ == "__main__":
    main()
