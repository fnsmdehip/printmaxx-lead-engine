#!/usr/bin/env python3

from __future__ import annotations
"""
EAS Lead Pipeline — filters PRINTMAXX scored leads for Enterprise Automation Solutions fit.

Reads scored leads from savvy_lead_scraper output, filters by EAS-specific criteria,
generates personalized cold email drafts, and outputs Instantly.ai-compatible CSV.

Usage:
    python3 eas_lead_pipeline.py --generate       # Generate EAS leads from scored data
    python3 eas_lead_pipeline.py --status          # Pipeline stats
    python3 eas_lead_pipeline.py --preview N       # Preview top N leads

Cron: 0 8 * * 1-5 cd $BASE && $PYTHON AUTOMATIONS/eas_lead_pipeline.py --generate >> AUTOMATIONS/logs/eas_pipeline.log 2>&1
"""

import csv
import json
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEADS_DIR = PROJECT_ROOT / "AUTOMATIONS" / "leads"
EAS_DIR = PROJECT_ROOT / "MONEY_METHODS" / "EAS"
OUTREACH_DIR = EAS_DIR / "outreach"
OUTREACH_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = OUTREACH_DIR / "eas_leads_ready.csv"
STATS_FILE = OUTREACH_DIR / "pipeline_stats.json"
SCORING_CONFIG = OUTREACH_DIR / "lead_scoring_config.json"

# EAS target industries (highest automation ROI)
TARGET_INDUSTRIES = {
    "dentist", "dental", "orthodontist", "periodontist",
    "lawyer", "attorney", "law firm", "legal",
    "plumber", "plumbing", "hvac", "heating", "cooling", "air conditioning",
    "electrician", "electrical",
    "contractor", "construction", "remodeling",
    "chiropractor", "physical therapy",
    "veterinarian", "vet", "animal hospital",
    "real estate", "realtor", "property management",
    "insurance", "insurance agent",
    "accounting", "accountant", "cpa", "bookkeeper",
    "auto repair", "mechanic", "body shop",
    "salon", "barbershop", "spa", "beauty",
    "restaurant", "catering",
    "marketing agency", "digital agency", "advertising",
}

# EAS-specific lead scoring weights
DEFAULT_SCORING = {
    "website_quality_inverse": 0.30,   # worse site = better lead for us
    "phone_first": 0.20,              # phone-heavy = needs phone automation
    "industry_fit": 0.20,             # target industries score higher
    "review_count": 0.15,             # established business = can pay
    "location_tier": 0.15,            # larger city = bigger operation
}

# Cold email templates
EMAIL_TEMPLATES = {
    "phone_heavy": {
        "subject": "your phones are costing you ${monthly_loss}/month",
        "body": """hi {first_name},

i looked at {business_name}'s setup and noticed something: {specific_issue}.

businesses like yours typically lose $2,000-4,000/month in missed calls alone. that's appointments that went to your competitor because nobody picked up after 5pm.

we set up AI phone concierges for {industry} businesses. the AI answers calls 24/7, books appointments into your calendar, and texts you when something needs a human.

takes 10 days. costs $3,500. most clients see payback in 60 days.

want to see how it works? i can show you a 15-minute demo with a {industry}-specific setup.

--
enterprise automation solutions
enterpriseautomation.solutions""",
    },
    "bad_website": {
        "subject": "your website is sending patients to competitors",
        "body": """hi {first_name},

i ran your site through our diagnostic tool and found {specific_issue}.

{business_name} has {review_count} reviews at {rating} stars. your reputation is solid. but your website is working against you.

we do a 5-day audit ($1,500) where we map your workflows, find where you're losing time and money, and give you an exact plan to fix it. most {industry} businesses recover 10-15 hours per week in manual tasks.

want the full diagnostic? takes 15 minutes to scope.

--
enterprise automation solutions
enterpriseautomation.solutions""",
    },
    "general": {
        "subject": "automating {industry} businesses, quick question",
        "body": """hi {first_name},

we help {industry} businesses automate their phones, meetings, and back-office tasks. things like missed calls, manual scheduling, and data entry between systems.

looking at {business_name}, i think we could save your team 8-12 hours per week.

we start with a $1,500 diagnostic sprint (5 days). we map your workflows, build an ROI model, and give you a concrete plan. if the numbers don't work, you keep the analysis and we part ways.

worth a 15-minute call to see if there's fit?

--
enterprise automation solutions
enterpriseautomation.solutions""",
    },
}


def load_scored_leads():
    """Load leads from savvy_lead_scraper output or nationwide_scraper output."""
    leads = []

    # Check multiple possible lead file locations
    lead_files = list(LEADS_DIR.glob("*.csv")) if LEADS_DIR.exists() else []
    lead_files += list((PROJECT_ROOT / "AUTOMATIONS" / "output").glob("**/*leads*.csv"))

    for lead_file in lead_files:
        try:
            with open(lead_file, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    leads.append(row)
        except Exception:
            continue

    return leads


def score_for_eas(lead):
    """Score a lead specifically for EAS fit (0-100)."""
    score = 0.0

    # Website quality inverse (worse site = better lead)
    website_score_raw = lead.get("website_score", lead.get("score", 50))
    try:
        website_score = float(website_score_raw) if website_score_raw and str(website_score_raw).strip() else 50
    except (ValueError, TypeError):
        website_score = 50
    score += (100 - website_score) * DEFAULT_SCORING["website_quality_inverse"]

    # Phone-first business
    has_phone = bool(lead.get("phone", lead.get("phone_number", "")))
    no_chat = "no" in str(lead.get("has_chat", "no")).lower()
    phone_score = 100 if (has_phone and no_chat) else (60 if has_phone else 20)
    score += phone_score * DEFAULT_SCORING["phone_first"]

    # Industry fit
    industry = str(lead.get("industry", lead.get("category", ""))).lower()
    industry_match = any(t in industry for t in TARGET_INDUSTRIES)
    score += (100 if industry_match else 30) * DEFAULT_SCORING["industry_fit"]

    # Review count (established = can pay)
    reviews = int(lead.get("review_count", lead.get("reviews", 0)) or 0)
    review_score = min(100, reviews * 2) if reviews > 10 else (reviews * 5)
    score += review_score * DEFAULT_SCORING["review_count"]

    # Location tier
    population = int(lead.get("city_population", lead.get("population", 50000)) or 50000)
    loc_score = min(100, population / 5000)
    score += loc_score * DEFAULT_SCORING["location_tier"]

    return round(score, 1)


def classify_lead(lead, _eas_score=None):
    """Determine which email template to use based on lead characteristics."""
    has_phone = bool(lead.get("phone", lead.get("phone_number", "")))
    website_score = float(lead.get("website_score", lead.get("score", 50)))

    if has_phone and website_score > 40:
        return "phone_heavy"
    elif website_score < 40:
        return "bad_website"
    else:
        return "general"


def generate_email(lead, template_key):
    """Generate personalized email from template."""
    template = EMAIL_TEMPLATES[template_key]
    business_name = lead.get("business_name", lead.get("name", "your business"))
    industry = lead.get("industry", lead.get("category", "local"))

    # Build specific issue callout
    website_score = float(lead.get("website_score", lead.get("score", 50)))
    issues = []
    if website_score < 30:
        issues.append("your site loads slowly and isn't mobile-friendly")
    elif website_score < 50:
        issues.append("your site is missing modern booking and chat features")
    if "no" in str(lead.get("has_ssl", "yes")).lower():
        issues.append("no SSL certificate (browsers show 'not secure' to visitors)")
    if not lead.get("has_chat", False):
        issues.append("no online booking or chat option")

    specific_issue = ". ".join(issues[:2]) if issues else "a few quick wins that could save your team hours per week"

    # Estimate monthly loss
    monthly_loss = "2,000" if website_score > 40 else "3,500"

    first_name = lead.get("contact_name", lead.get("owner", "")).split()[0] if lead.get("contact_name", lead.get("owner", "")) else "there"

    subject = template["subject"].format(
        monthly_loss=monthly_loss,
        industry=industry,
    )
    body = template["body"].format(
        first_name=first_name,
        business_name=business_name,
        industry=industry,
        specific_issue=specific_issue,
        review_count=lead.get("review_count", lead.get("reviews", "multiple")),
        rating=lead.get("rating", "4.0+"),
        monthly_loss=monthly_loss,
    )
    return subject, body


def generate_pipeline():
    """Main pipeline: load leads → score → filter → generate emails → export CSV."""
    leads = load_scored_leads()
    if not leads:
        print("No leads found. Run savvy_lead_scraper.py or nationwide_scraper.py first.")
        print(f"Checked: {LEADS_DIR} and AUTOMATIONS/output/")
        return

    # Score all leads for EAS
    scored = []
    for lead in leads:
        eas_score = score_for_eas(lead)
        if eas_score >= 45:  # minimum threshold for EAS outreach
            lead["eas_score"] = eas_score
            lead["email_template"] = classify_lead(lead, eas_score)
            scored.append(lead)

    # Sort by EAS score descending
    scored.sort(key=lambda x: x["eas_score"], reverse=True)

    # Generate emails and export
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "business_name", "email", "phone", "industry", "website",
            "eas_score", "email_subject", "email_body", "template_used",
        ])
        for lead in scored[:100]:  # cap at 100 per batch
            subject, body = generate_email(lead, lead["email_template"])
            writer.writerow([
                lead.get("business_name", lead.get("name", "")),
                lead.get("email", ""),
                lead.get("phone", lead.get("phone_number", "")),
                lead.get("industry", lead.get("category", "")),
                lead.get("website", lead.get("url", "")),
                lead["eas_score"],
                subject,
                body,
                lead["email_template"],
            ])

    # Save stats
    stats = {
        "total_leads_scanned": len(leads),
        "eas_qualified": len(scored),
        "exported": min(len(scored), 100),
        "by_template": {
            "phone_heavy": len([s for s in scored if s["email_template"] == "phone_heavy"]),
            "bad_website": len([s for s in scored if s["email_template"] == "bad_website"]),
            "general": len([s for s in scored if s["email_template"] == "general"]),
        },
        "avg_eas_score": round(sum(s["eas_score"] for s in scored) / len(scored), 1) if scored else 0,
        "generated_at": datetime.now().isoformat(),
    }
    with open(STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"EAS Lead Pipeline: {len(leads)} scanned → {len(scored)} qualified → {min(len(scored), 100)} exported")
    print(f"Output: {OUTPUT_CSV}")
    print(f"By template: phone_heavy={stats['by_template']['phone_heavy']}, bad_website={stats['by_template']['bad_website']}, general={stats['by_template']['general']}")


def show_status():
    """Show pipeline statistics."""
    if STATS_FILE.exists():
        stats = json.loads(STATS_FILE.read_text())
        print(f"\n=== EAS Lead Pipeline Status ===")
        print(f"Last run: {stats.get('generated_at', 'never')}")
        print(f"Leads scanned: {stats.get('total_leads_scanned', 0)}")
        print(f"EAS qualified: {stats.get('eas_qualified', 0)}")
        print(f"Exported: {stats.get('exported', 0)}")
        print(f"Avg EAS score: {stats.get('avg_eas_score', 0)}")
        templates = stats.get("by_template", {})
        print(f"Templates: phone_heavy={templates.get('phone_heavy', 0)}, bad_website={templates.get('bad_website', 0)}, general={templates.get('general', 0)}")
    else:
        print("No pipeline stats yet. Run --generate first.")


def preview_leads(n=10):
    """Preview top N leads from last run."""
    if not OUTPUT_CSV.exists():
        print("No leads exported yet. Run --generate first.")
        return
    with open(OUTPUT_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= n:
                break
            print(f"\n--- Lead {i+1} (EAS Score: {row['eas_score']}) ---")
            print(f"  Business: {row['business_name']}")
            print(f"  Industry: {row['industry']}")
            print(f"  Template: {row['template_used']}")
            print(f"  Subject: {row['email_subject']}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--generate" in args:
        generate_pipeline()
    elif "--status" in args:
        show_status()
    elif "--preview" in args:
        n = int(args[args.index("--preview") + 1]) if len(args) > args.index("--preview") + 1 else 10
        preview_leads(n)
    else:
        print("Usage: eas_lead_pipeline.py --generate | --status | --preview N")
