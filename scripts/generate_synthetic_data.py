"""
Generate synthetic CSV data for customers, credit_usage, and billing tables.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
NUM_CUSTOMERS = 100
NUM_CREDIT_RECORDS = 500
NUM_BILLING_RECORDS = 200

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Sample data
CUSTOMER_NAMES = [
    "Acme Corp", "TechStart Inc", "Global Solutions", "DataFlow Systems",
    "CloudNine Ltd", "InnovateCo", "Quantum Analytics", "Vertex Technologies",
    "Fusion Dynamics", "Alpha Ventures", "Beta Systems", "Gamma Industries",
    "Delta Innovations", "Epsilon Corp", "Zeta Solutions", "Eta Technologies",
    "Theta Analytics", "Iota Systems", "Kappa Corp", "Lambda Innovations"
]

TIERS = ["Free", "Basic", "Pro", "Enterprise"]
STATUSES = ["Active", "Active", "Active", "Active", "Churned"]  # 80% active
FEATURES = ["API_Calls", "Storage", "Compute", "Analytics", "ML_Training", "Data_Transfer"]


def generate_customers():
    """Generate synthetic customer data."""
    customers = []
    base_date = datetime(2023, 1, 1)
    
    for i in range(1, NUM_CUSTOMERS + 1):
        signup_days = random.randint(0, 700)
        signup_date = (base_date + timedelta(days=signup_days)).strftime("%Y-%m-%d")
        
        customer = {
            "customer_id": f"CUST_{i:04d}",
            "name": f"{random.choice(CUSTOMER_NAMES)} {i}" if i > len(CUSTOMER_NAMES) else f"{CUSTOMER_NAMES[i-1] if i <= len(CUSTOMER_NAMES) else CUSTOMER_NAMES[0]} {i}",
            "signup_date": signup_date,
            "tier": random.choice(TIERS),
            "status": random.choice(STATUSES),
            "industry": random.choice(["Technology", "Finance", "Healthcare", "Retail", "Manufacturing"]),
            "region": random.choice(["North America", "Europe", "Asia Pacific", "Latin America"])
        }
        customers.append(customer)
    
    # Write to CSV
    with open(DATA_DIR / "customers.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)
    
    print(f"Generated {len(customers)} customer records -> {DATA_DIR / 'customers.csv'}")
    return customers


def generate_credit_usage(customers):
    """Generate synthetic credit usage data."""
    usage_records = []
    base_date = datetime(2023, 1, 1)
    
    for i in range(1, NUM_CREDIT_RECORDS + 1):
        customer = random.choice(customers)
        # Only active customers use credits
        if customer["status"] != "Active" and random.random() > 0.1:
            continue
            
        usage_days = random.randint(0, 700)
        usage_date = (base_date + timedelta(days=usage_days)).strftime("%Y-%m-%d")
        
        # Credit usage varies by tier
        tier_multiplier = {"Free": 1, "Basic": 5, "Pro": 20, "Enterprise": 100}
        base_credits = random.uniform(10, 1000)
        credits_used = base_credits * tier_multiplier.get(customer["tier"], 1)
        
        usage = {
            "usage_id": f"USG_{i:05d}",
            "customer_id": customer["customer_id"],
            "date": usage_date,
            "credits_used": round(credits_used, 2),
            "feature": random.choice(FEATURES),
            "execution_time_ms": random.randint(100, 5000)
        }
        usage_records.append(usage)
    
    # Write to CSV
    with open(DATA_DIR / "credit_usage.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=usage_records[0].keys())
        writer.writeheader()
        writer.writerows(usage_records)
    
    print(f"Generated {len(usage_records)} credit usage records -> {DATA_DIR / 'credit_usage.csv'}")


def generate_billing(customers):
    """Generate synthetic billing data."""
    billing_records = []
    base_date = datetime(2023, 1, 1)
    
    for i in range(1, NUM_BILLING_RECORDS + 1):
        customer = random.choice(customers)
        
        # Generate billing date
        billing_months = random.randint(0, 23)
        billing_date = (base_date + timedelta(days=billing_months * 30)).strftime("%Y-%m-%d")
        
        # Amount varies by tier
        tier_amount = {
            "Free": 0,
            "Basic": random.uniform(99, 199),
            "Pro": random.uniform(499, 999),
            "Enterprise": random.uniform(2000, 10000)
        }
        amount = tier_amount.get(customer["tier"], 0)
        
        billing = {
            "billing_id": f"BILL_{i:05d}",
            "customer_id": customer["customer_id"],
            "billing_date": billing_date,
            "amount": round(amount, 2),
            "period": f"{(base_date + timedelta(days=billing_months * 30)).strftime('%Y-%m')}",
            "status": random.choice(["Paid", "Paid", "Paid", "Pending", "Overdue"]),
            "currency": "USD"
        }
        billing_records.append(billing)
    
    # Write to CSV
    with open(DATA_DIR / "billing.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=billing_records[0].keys())
        writer.writeheader()
        writer.writerows(billing_records)
    
    print(f"Generated {len(billing_records)} billing records -> {DATA_DIR / 'billing.csv'}")


def main():
    """Main function to generate all synthetic data."""
    print("Generating synthetic data...")
    print(f"Output directory: {DATA_DIR}")
    print("-" * 50)
    
    customers = generate_customers()
    generate_credit_usage(customers)
    generate_billing(customers)
    
    print("-" * 50)
    print("Data generation complete!")


if __name__ == "__main__":
    main()
