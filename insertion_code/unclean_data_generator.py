import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import string

# Initialize Faker and set seed for reproducibility
fake = Faker()
np.random.seed(42)
random.seed(42)

# Configuration
num_rows = 10_000_000  # Number of rows to generate
output_file = "D:\Projects\working_data\data_creatorsynthetic_bank_customers_transactions.csv"

# Helper functions
def parse_relative_date(rel_str, ref_date=None):
    """Parse relative date strings like '-18y' or '-2y'"""
    if ref_date is None:
        ref_date = datetime.now()
    if rel_str.endswith('y'):
        years = int(rel_str[:-1])
        return ref_date - timedelta(days=365 * years)
    elif rel_str.endswith('m'):
        months = int(rel_str[:-1])
        return ref_date - timedelta(days=30 * months)
    elif rel_str.endswith('d'):
        days = int(rel_str[:-1])
        return ref_date - timedelta(days=days)
    return ref_date

def random_date(start_date, end_date):
    """Generate random date between two dates"""
    # Ensure start_date is before end_date
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    time_between = end_date - start_date
    random_days = random.randrange(time_between.days)
    return start_date + timedelta(days=random_days)

def introduce_typos(text, prob=0.05):
    """Randomly introduce typos in text"""
    if not text or random.random() > prob:
        return text
    text = list(text)
    pos = random.randint(0, len(text)-1)
    action = random.choice(["delete", "insert", "replace"])
    if action == "delete":
        text.pop(pos)
    elif action == "insert":
        text.insert(pos, random.choice(string.ascii_letters))
    else:
        text[pos] = random.choice(string.ascii_letters)
    return "".join(text)

def maybe_add_special_chars(text, prob=0.03):
    """Sometimes add special characters"""
    if not text or random.random() > prob:
        return text
    special_chars = ["@", "#", "$", "%", "&", "*", "(", ")", "-", "_", "+", "="]
    pos = random.randint(0, len(text))
    text = text[:pos] + random.choice(special_chars) + text[pos:]
    return text

def corrupt_data(value, prob=0.02):
    """Sometimes completely corrupt the data"""
    if value and random.random() < prob:
        return "".join(random.choices(string.printable, k=random.randint(3, 10)))
    return value

# Define fixed date ranges (1990-2025)
min_date = datetime(1990, 1, 1)
max_date = datetime(2025, 12, 31)

# Column definitions with proper date ranges
column_definitions = {
    # Customer demographics (messy)
    "customer_id": {"type": "str", "null_rate": 0.0},
    "first_name": {"type": "name", "null_rate": 0.02, "variations": ["FIRST_NAME", "Firstname", "FName"]},
    "last_name": {"type": "name", "null_rate": 0.02, "variations": ["LAST_NAME", "Lastname", "LName"]},
    "full_name": {"type": "composite", "null_rate": 0.05},
    "gender": {"type": "categorical", "options": ["M", "F", "Male", "Female", "m", "f", "U", "Unknown", np.nan], "null_rate": 0.1},
    "dob": {"type": "date", "start": min_date, "end": max_date, "null_rate": 0.05, "formats": ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%y", "%Y%m%d"]},
    "age": {"type": "derived", "null_rate": 0.08},
    
    # Contact info (messy with variations)
    "email": {"type": "email", "null_rate": 0.03},
    "phone": {"type": "phone", "null_rate": 0.04, "formats": ["###-###-####", "(###) ###-####", "##########", "###.###.####"]},
    "address": {"type": "address", "null_rate": 0.06},
    "city": {"type": "city", "null_rate": 0.05, "variations": {"New York": ["NYC", "New York City", "N.Y."]}},
    "state": {"type": "state", "null_rate": 0.03, "abbreviate_prob": 0.4},
    "zip_code": {"type": "zip", "null_rate": 0.04, "extended_prob": 0.3},
    
    # Financial profile (with outliers)
    "credit_score": {"type": "int", "min": 300, "max": 850, "null_rate": 0.07, "outlier_prob": 0.01},
    "annual_income": {"type": "float", "min": 10000, "max": 500000, "null_rate": 0.08, "outlier_prob": 0.02},
    "employment_status": {"type": "categorical", "options": ["Employed", "Unemployed", "Self-employed", "Retired", "Student", "Part-time", "Unknown", np.nan], "null_rate": 0.1},
    
    # Account details
    "account_open_date": {"type": "date", "start": min_date, "end": max_date, "null_rate": 0.03, "formats": ["%Y-%m-%d", "%m/%d/%Y"]},
    "account_type": {"type": "categorical", "options": ["Checking", "Savings", "Premium", "Student", "Joint", "Business", "CHK", "SVG"], "null_rate": 0.04},
    "account_balance": {"type": "float", "min": -500, "max": 500000, "null_rate": 0.05},
    
    # Transaction data
    "transaction_id": {"type": "str", "null_rate": 0.0},
    "transaction_date": {"type": "date", "start": min_date, "end": max_date, "null_rate": 0.02, "formats": ["%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%Y%m%d%H%M%S"]},
    "transaction_amount": {"type": "float", "min": 0.01, "max": 10000, "null_rate": 0.01, "outlier_prob": 0.005},
    "transaction_type": {"type": "categorical", "options": ["Purchase", "Withdrawal", "Deposit", "Transfer", "Payment", "PUR", "W/D", "DEP", "XFR"], "null_rate": 0.03},
    "merchant_name": {"type": "company", "null_rate": 0.15},
    "merchant_category": {"type": "categorical", "options": ["Retail", "Groceries", "Dining", "Travel", "Entertainment", "Gas", "Online", "Other", "UNKNOWN"], "null_rate": 0.1},
    
    # Payment details (messy with fake sensitive data)
    "payment_method": {"type": "categorical", "options": ["Credit Card", "Debit Card", "ACH", "Check", "CC", "DC", "CHK"], "null_rate": 0.05},
    "card_last_four": {"type": "str", "template": "####", "null_rate": 0.3},
    "card_type": {"type": "categorical", "options": ["Visa", "Mastercard", "Amex", "Discover", "VISA", "MC", "AMEX", "DISC", None], "null_rate": 0.35},
    
    # Fraud flags and suspicious indicators
    "is_flagged": {"type": "bool", "true_prob": 0.01, "null_rate": 0.02},
    "is_fraud": {"type": "bool", "true_prob": 0.002, "null_rate": 0.0},
    "risk_score": {"type": "float", "min": 0, "max": 1, "null_rate": 0.1},
    
    # Useless/redundant columns
    "notes": {"type": "text", "null_rate": 0.7, "max_length": 100},
    "internal_comments": {"type": "text", "null_rate": 0.8, "max_length": 50},
    "temp_data": {"type": "junk", "null_rate": 0.95},
    "unused_field": {"type": "junk", "null_rate": 0.99},
    "legacy_id": {"type": "str", "null_rate": 0.6, "template": "ID-#####"},
}

# Generate the dataset
data = {}

print("Generating synthetic dataset...")

# Generate each column
for col, config in column_definitions.items():
    print(f"Generating column: {col}")
    
    if config["type"] == "str":
        data[col] = [f"ID-{i:08d}" if random.random() > config["null_rate"] else np.nan for i in range(num_rows)]
    
    elif config["type"] == "name":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                name = fake.first_name() if col == "first_name" else fake.last_name()
                if random.random() < 0.1 and "variations" in config:
                    name = random.choice(config["variations"])
                name = introduce_typos(name)
                data[col].append(name)
    
    elif config["type"] == "composite":
        data[col] = []
        for i in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                first = data["first_name"][i] if "first_name" in data and not pd.isna(data["first_name"][i]) else fake.first_name()
                last = data["last_name"][i] if "last_name" in data and not pd.isna(data["last_name"][i]) else fake.last_name()
                fmt = random.choice(["{first} {last}", "{last}, {first}", "{first[0]}. {last}", "{first}-{last}"])
                full_name = fmt.format(first=first, last=last)
                full_name = introduce_typos(full_name)
                data[col].append(full_name)
    
    elif config["type"] == "categorical":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                value = random.choice(config["options"])
                value = corrupt_data(value)
                data[col].append(value)
    
    elif config["type"] == "date":
        data[col] = []
        start = config["start"] if isinstance(config["start"], datetime) else parse_relative_date(config["start"])
        end = config["end"] if isinstance(config["end"], datetime) else parse_relative_date(config["end"])
        
        # Ensure dates are within our fixed range (1990-2025)
        start = max(start, min_date)
        end = min(end, max_date)
        
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                date = random_date(start, end)
                fmt = random.choice(config["formats"])
                try:
                    date_str = date.strftime(fmt)
                except:
                    date_str = date.strftime("%Y-%m-%d")
                if random.random() < 0.01:
                    date_str = corrupt_data(date_str)
                data[col].append(date_str)
    
    elif config["type"] == "int":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                value = random.randint(config["min"], config["max"])
                if random.random() < config.get("outlier_prob", 0):
                    value = value * random.choice([10, -1, 100, 0])
                if random.random() < 0.02:
                    value = str(value) + random.choice(["", " ", "a", "!", "USD"])
                data[col].append(value)
    
    elif config["type"] == "float":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                value = round(random.uniform(config["min"], config["max"]), 2)
                if random.random() < config.get("outlier_prob", 0):
                    value = value * random.choice([10, -1, 100, 0])
                if random.random() < 0.03:
                    value = f"${value}"
                elif random.random() < 0.02:
                    value = str(value).replace(".", ",")
                data[col].append(value)
    
    elif config["type"] == "bool":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                value = random.random() < config["true_prob"]
                if random.random() < 0.1:
                    value = random.choice(["Yes", "No"]) if value else random.choice(["Y", "N", "T", "F"])
                data[col].append(value)
    
    elif config["type"] == "email":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                email = fake.email()
                if random.random() < 0.05:
                    email = email.replace("@", "#")
                elif random.random() < 0.03:
                    email = email.split("@")[0]
                data[col].append(email)
    
    elif config["type"] == "phone":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                fmt = random.choice(config["formats"])
                phone = ""
                for c in fmt:
                    if c == "#":
                        phone += str(random.randint(0, 9))
                    else:
                        phone += c
                if random.random() < 0.04:
                    phone = phone[:random.randint(3, len(phone)-1)] + "x" + phone[random.randint(3, len(phone)-1):]
                data[col].append(phone)
    
    elif config["type"] == "address":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                address = fake.street_address()
                if random.random() < 0.3:
                    address += f" {random.choice(['Apt', 'Suite', 'Unit'])} {random.randint(1, 300)}"
                address = introduce_typos(address)
                data[col].append(address)
    
    elif config["type"] == "city":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                city = fake.city()
                if "variations" in config and random.random() < 0.1:
                    for original, variants in config["variations"].items():
                        if city == original:
                            city = random.choice(variants)
                            break
                city = introduce_typos(city)
                data[col].append(city)
    
    elif config["type"] == "state":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                if random.random() < config["abbreviate_prob"]:
                    state = fake.state_abbr()
                else:
                    state = fake.state()
                data[col].append(state)
    
    elif config["type"] == "zip":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                if random.random() < config["extended_prob"]:
                    zip_code = fake.zipcode_plus4()
                else:
                    zip_code = fake.zipcode()
                if random.random() < 0.05:
                    zip_code = zip_code.replace("-", " ")
                elif random.random() < 0.03:
                    zip_code = zip_code[:3]
                data[col].append(zip_code)
    
    elif config["type"] == "company":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                company = fake.company()
                if random.random() < 0.2:
                    company = random.choice(["LLC", "Inc", "Corp"]) + " " + company
                elif random.random() < 0.1:
                    company = company + " " + random.choice(["LLC", "Inc", "Corp", "Ltd"])
                company = introduce_typos(company)
                data[col].append(company)
    
    elif config["type"] == "text":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                text = fake.text(max_nb_chars=config["max_length"])
                text = maybe_add_special_chars(text)
                data[col].append(text)
    
    elif config["type"] == "junk":
        data[col] = []
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                junk = "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 15)))
                data[col].append(junk)
    
    elif config["type"] == "template":
        data[col] = []
        template = config["template"]
        for _ in range(num_rows):
            if random.random() < config["null_rate"]:
                data[col].append(np.nan)
            else:
                value = ""
                for c in template:
                    if c == "#":
                        value += str(random.randint(0, 9))
                    else:
                        value += c
                data[col].append(value)
    
    elif config["type"] == "derived":
        if col == "age":
            data[col] = []
            for i in range(num_rows):
                if random.random() < config["null_rate"] or pd.isna(data["dob"][i]):
                    data[col].append(np.nan)
                else:
                    dob_str = data["dob"][i]
                    dob = None
                    for fmt in column_definitions["dob"]["formats"]:
                        try:
                            dob = datetime.strptime(dob_str, fmt)
                            break
                        except:
                            continue
                    if dob is None:
                        data[col].append(np.nan)
                    else:
                        today = datetime.now()
                        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                        if random.random() < 0.02:
                            age = str(age) + random.choice([" years", "yo", " yrs"])
                        data[col].append(age)

# Create DataFrame
df = pd.DataFrame(data)

# Introduce some duplicates (about 0.1% of rows)
num_duplicates = int(num_rows * 0.001)
duplicate_indices = random.sample(range(num_rows), num_duplicates)
for idx in duplicate_indices:
    df.loc[idx] = df.loc[random.randint(0, num_rows-1)]

# Save to CSV
print("Saving to CSV...")
df.to_csv(output_file, index=False)

print(f"Dataset generated with {num_rows} rows and {len(column_definitions)} columns. Saved to {output_file}")

