"""
Generate synthetic SaaS datasets for Snowflake Business Analytics demos.

Outputs (by default into ./data):
  - customers.csv   : account-level attributes
  - usage_logs.csv  : daily credit consumption + active users
  - billing.csv     : monthly charges derived from usage + plan pricing
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path


PLANS = {
    "STARTER": {"base_usd": 199, "included_credits": 200, "overage_per_credit": 1.25},
    "GROWTH": {"base_usd": 699, "included_credits": 900, "overage_per_credit": 1.05},
    "ENTERPRISE": {"base_usd": 2499, "included_credits": 4000, "overage_per_credit": 0.85},
}

INDUSTRIES = [
    "FinTech",
    "E-Commerce",
    "Healthcare",
    "Education",
    "Media",
    "Developer Tools",
    "Cybersecurity",
    "Manufacturing",
]

REGIONS = ["NA", "EMEA", "APAC", "LATAM"]


@dataclass(frozen=True)
class Customer:
    account_id: str
    customer_name: str
    plan_tier: str
    industry: str
    region: str
    signup_date: date


def _daterange(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end must be >= start")
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _add_months(d: date, months: int) -> date:
    # Month arithmetic without external deps
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    return date(year, month, 1)


def _month_starts(start_month: date, end_month: date) -> list[date]:
    cur = _month_start(start_month)
    end = _month_start(end_month)
    out: list[date] = []
    while cur <= end:
        out.append(cur)
        cur = _add_months(cur, 1)
    return out


def _safe_float(x: float) -> float:
    if math.isnan(x) or math.isinf(x):
        return 0.0
    return float(x)


def generate_customers(
    *,
    rng: random.Random,
    n_customers: int,
    start_date: date,
    end_date: date,
) -> list[Customer]:
    customers: list[Customer] = []
    for i in range(1, n_customers + 1):
        account_id = f"CUST_{i:05d}"
        plan_tier = rng.choices(
            population=list(PLANS.keys()),
            weights=[0.55, 0.3, 0.15],
            k=1,
        )[0]
        industry = rng.choice(INDUSTRIES)
        region = rng.choice(REGIONS)
        # Signup biased toward earlier dates so most customers have usage history
        signup = start_date + timedelta(days=int(rng.random() ** 2 * (end_date - start_date).days))
        customers.append(
            Customer(
                account_id=account_id,
                customer_name=f"Acme {industry} {i}",
                plan_tier=plan_tier,
                industry=industry,
                region=region,
                signup_date=signup,
            )
        )
    return customers


def generate_usage_logs(
    *,
    rng: random.Random,
    customers: list[Customer],
    start_date: date,
    end_date: date,
    churn_risk_rate: float,
) -> list[dict[str, object]]:
    """
    Generate daily usage with seasonality + customer-specific trend.
    Some customers are assigned an induced ~20%+ decline in the last month to
    make churn-risk detection meaningful.
    """
    all_days = _daterange(start_date, end_date)
    last_month_start = _month_start(end_date)
    prior_month_start = _add_months(last_month_start, -1)

    churn_risk_set = {
        c.account_id for c in rng.sample(customers, k=max(1, int(len(customers) * churn_risk_rate)))
    }

    rows: list[dict[str, object]] = []
    for c in customers:
        # Baseline is scaled by plan tier
        plan_scale = {"STARTER": 1.0, "GROWTH": 2.0, "ENTERPRISE": 4.0}[c.plan_tier]
        base_daily_credits = rng.uniform(2.0, 12.0) * plan_scale
        base_active_users = rng.uniform(5, 30) * plan_scale

        # Customer-specific weekly seasonality + gentle drift
        weekly_amp = rng.uniform(0.05, 0.25)
        drift = rng.uniform(-0.002, 0.004)  # per day multiplicative drift
        noise_sigma = rng.uniform(0.08, 0.22)

        for d in all_days:
            if d < c.signup_date:
                continue

            day_index = (d - start_date).days
            dow = d.weekday()

            weekly = 1.0 + weekly_amp * math.sin(2 * math.pi * (dow / 7.0))
            trend = (1.0 + drift) ** day_index
            noise = rng.lognormvariate(mu=0.0, sigma=noise_sigma)  # multiplicative

            credits = base_daily_credits * weekly * trend * noise
            act_users = base_active_users * weekly * (0.8 + 0.4 * rng.random())

            # Induce a visible decline for churn-risk accounts in the last month
            if c.account_id in churn_risk_set and d >= last_month_start:
                credits *= rng.uniform(0.65, 0.78)
                act_users *= rng.uniform(0.70, 0.85)

            # Slight bump for everyone in the most recent month vs prior (macro growth)
            if d >= last_month_start and c.account_id not in churn_risk_set:
                credits *= rng.uniform(1.02, 1.12)

            # Clamp and round
            credits = round(max(0.0, _safe_float(credits)), 3)
            act_users = int(max(0, round(_safe_float(act_users))))

            rows.append(
                {
                    "usage_date": d.isoformat(),
                    "account_id": c.account_id,
                    "credits_used": credits,
                    "active_users": act_users,
                }
            )

    # Ensure churn-risk set has prior+last month data for detection if possible
    _ = prior_month_start  # for readability; logic covered by generated date range
    return rows


def generate_billing(
    *,
    customers: list[Customer],
    usage_logs: list[dict[str, object]],
    start_date: date,
    end_date: date,
) -> list[dict[str, object]]:
    # Aggregate monthly credits per account
    monthly: dict[tuple[str, str], float] = {}
    for r in usage_logs:
        month = str(r["usage_date"])[:7]  # YYYY-MM
        key = (str(r["account_id"]), month)
        monthly[key] = monthly.get(key, 0.0) + float(r["credits_used"])

    months = _month_starts(_month_start(start_date), _month_start(end_date))
    rows: list[dict[str, object]] = []

    plan_by_account = {c.account_id: c.plan_tier for c in customers}
    for c in customers:
        plan = PLANS[plan_by_account[c.account_id]]
        for m in months:
            month_key = f"{m.year:04d}-{m.month:02d}"
            credits = float(monthly.get((c.account_id, month_key), 0.0))
            included = float(plan["included_credits"])
            overage_credits = max(0.0, credits - included)
            overage_usd = overage_credits * float(plan["overage_per_credit"])
            amount = float(plan["base_usd"]) + overage_usd
            rows.append(
                {
                    "billing_month": month_key,
                    "account_id": c.account_id,
                    "plan_tier": plan_by_account[c.account_id],
                    "credits_billed": round(credits, 3),
                    "base_fee_usd": round(float(plan["base_usd"]), 2),
                    "overage_usd": round(overage_usd, 2),
                    "amount_usd": round(amount, 2),
                }
            )
    return rows


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic SaaS CSVs for Snowflake demos.")
    parser.add_argument("--output-dir", default="data", help="Directory to write CSVs (default: data)")
    parser.add_argument("--customers", type=int, default=250, help="Number of customers (default: 250)")
    parser.add_argument(
        "--start-date",
        default=None,
        help="Start date YYYY-MM-DD (default: 365 days ago)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument("--seed", type=int, default=7, help="Random seed (default: 7)")
    parser.add_argument(
        "--churn-risk-rate",
        type=float,
        default=0.08,
        help="Fraction of customers with induced decline (default: 0.08)",
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else date.today()
    start = (
        datetime.strptime(args.start_date, "%Y-%m-%d").date()
        if args.start_date
        else (end - timedelta(days=365))
    )
    if start >= end:
        raise SystemExit("start-date must be earlier than end-date")

    customers = generate_customers(rng=rng, n_customers=args.customers, start_date=start, end_date=end)
    usage_logs = generate_usage_logs(
        rng=rng,
        customers=customers,
        start_date=start,
        end_date=end,
        churn_risk_rate=args.churn_risk_rate,
    )
    billing = generate_billing(customers=customers, usage_logs=usage_logs, start_date=start, end_date=end)

    out_dir = Path(args.output_dir)
    customers_path = out_dir / "customers.csv"
    usage_path = out_dir / "usage_logs.csv"
    billing_path = out_dir / "billing.csv"

    _write_csv(
        customers_path,
        [
            {
                "account_id": c.account_id,
                "customer_name": c.customer_name,
                "plan_tier": c.plan_tier,
                "industry": c.industry,
                "region": c.region,
                "signup_date": c.signup_date.isoformat(),
            }
            for c in customers
        ],
        ["account_id", "customer_name", "plan_tier", "industry", "region", "signup_date"],
    )
    _write_csv(
        usage_path,
        usage_logs,
        ["usage_date", "account_id", "credits_used", "active_users"],
    )
    _write_csv(
        billing_path,
        billing,
        [
            "billing_month",
            "account_id",
            "plan_tier",
            "credits_billed",
            "base_fee_usd",
            "overage_usd",
            "amount_usd",
        ],
    )

    print("Generated:")
    print(f"  - {customers_path}")
    print(f"  - {usage_path}")
    print(f"  - {billing_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

