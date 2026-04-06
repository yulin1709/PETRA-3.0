# PETRA 3.0 — Predictive Engine for TRMS Risk & Alerts

**Built for Petronas Trading Digital, Service Management Team**

PETRA is a daily-run Python tool that reads your myGenie+ incident data, calculates SLA status, classifies every ticket by issue type, predicts which open tickets are likely to breach their SLA, and produces colour-coded Excel reports and an interactive HTML dashboard — all automatically.

---

## What PETRA Does in Plain English

Every morning you run one command (`python main.py`). PETRA then:

1. Reads the latest incident extract from your shared drive
2. Calculates how many working hours each ticket has consumed (09:00–midnight window, excluding weekends and Malaysian public holidays)
3. Labels each ticket S1 / S2 / S3 / S4 / S4+ (Breach) based on those hours
4. Reads the ticket descriptions and automatically categorises each one (Wrong Price, Report Issue, Push to SAP, etc.)
5. Uses a machine learning model trained on your 3 years of historical data to score every open ticket with a **breach risk %**
6. Flags tickets as **AT RISK**, **Already Breached**, **Monitor**, or **Low Risk**
7. Detects **zombie tickets** — tickets that have been open for more than twice their SLA target and appear to have been forgotten
8. Predicts the **exact datetime** each open ticket is expected to breach
9. Exports two Excel reports and one HTML dashboard
10. Prints a ready-to-paste Microsoft Teams alert message

---

## File Structure

```
PETRA-2.0-main/
│
├── sla.py              ← Step 1: Calculates working hours and SLA status
├── classifier.py       ← Step 2: Categorises tickets by issue type
├── main.py             ← Step 3: Runs everything, trains the model, exports reports
├── dashboard.py        ← Step 4: Generates the interactive HTML dashboard
└── README.md           ← This file
```

---

## What Each File Does

---

### `sla.py` — SLA Calculator

**What it does:**
This script reads the raw incident Excel file and works out, for every ticket, how many working hours have passed since it was reported. It then assigns a severity label.

**Working hours definition:**
- One working day = 9:00 AM to midnight (15 hours)
- Weekends are excluded
- Malaysian public holidays (from `Holiday.csv`) are excluded

**SLA thresholds:**

| Severity | Limit | Meaning |
|---|---|---|
| S1 | 6 working hours | Critical — must resolve same day |
| S2 | 12 working hours | High — must resolve within 1 working day |
| S3 | 105 working hours | Medium — 7 working days |
| S4 | 210 working hours | Low — 14 working days |
| S4+ (Breach) | Over 210 hours | SLA already missed |

**What it produces:**
An enriched Excel file (`Incident_SLA_All_Years.xlsx`) with new columns including `Resolution_WorkingHours`, `Severity`, `SLA_Met`, and `Resolution_Bucket`.

**If you want to change the SLA thresholds:**
Open `sla.py` and find this section near the top:
```python
SLA_THRESHOLDS = {
    "S1":   6,
    "S2":  12,
    "S3": 105,
    "S4": 210,
}
```
Change the numbers to match your new agreed thresholds. The rest of the script adjusts automatically.

**If you want to change the working hours window (e.g. 8am–6pm instead of 9am–midnight):**
Find these two lines:
```python
BUSINESS_START     = time(9, 0)
WORK_HOURS_PER_DAY = 15
```
Change `time(9, 0)` to your start time and update `WORK_HOURS_PER_DAY` to match the number of hours in your new window. You will also need to update the `we` (window end) line inside `working_hours_between()`.

**If you want to add more holidays:**
Open `Holiday.csv` and add rows in the same format: `Year, Month, Day`. The script reads this file fresh every run.

---

### `classifier.py` — Issue Type Classifier

**What it does:**
This script reads the ticket descriptions and automatically assigns each ticket to an issue category such as "Wrong Price", "Report Issue", "Push to SAP", "Missing Data", etc. It also assigns a confidence level (HIGH / MEDIUM / LOW) to each classification.

**How the classification works (non-technical explanation):**
Think of it like a very detailed checklist. For each ticket description, the script checks whether certain words or phrases appear. Each matching phrase adds points to a category. The category with the most points wins. For example:

- If a description contains "wrong" AND "price" close together → adds 11 points to "Wrong Price"
- If it contains "push" AND "NE" or "JE" → adds 12 points to "Push to SAP"
- If nothing matches strongly → the ticket lands in "Others"

**Confidence levels:**
- HIGH (≥10 points): The classifier is very sure
- MEDIUM (6–9 points): Reasonably confident
- LOW (<6 points): Weak match — worth a manual check

**What it produces:**
A file called `Classified_Incidents.xlsx` with new columns: `Refined Summary` (the category), `Reason` (which phrases triggered it), `Match Debug` (top 3 scoring categories), and `Confidence`.

**If you want to add a new category:**
Open `classifier.py` and find the `PATTERNS` dictionary. Add a new entry following the same format:
```python
"Your New Category": [
    (r"\bkeyword1\b.{0,30}\bkeyword2\b", 10, "description of this rule"),
    (r"\banother phrase\b", 8, "description"),
],
```
Then add your new category name to the `CATS` list and the `PRIORITY` list (lower in the list = lower priority when there's a tie).

**If Wrong Price is still over-classifying:**
The patterns for Wrong Price now require BOTH a problem word (wrong, incorrect, mismatch) AND a price-related term to appear close together. If you still see false positives, look at the `Match Debug` column in the output — it shows you which rule fired. You can then tighten that specific rule.

**If too many tickets are landing in "Others":**
Check the quality metrics printed at the end of each run. If Others > 20%, the script will warn you. Look at a sample of those tickets and identify common phrases, then add new patterns for them.

---

### `main.py` — The Brain (ML Pipeline + Reports)

**What it does:**
This is the master script that runs everything in sequence. It calls `sla.py` and `classifier.py`, merges their outputs, engineers features, trains a machine learning model, scores every open ticket with a breach risk percentage, and exports all reports.

**How the prediction works (non-technical explanation):**

Imagine you have 3 years of resolved tickets. For each one, you know: what type of issue it was, what severity it was, what time of day it came in, which team handled it, and whether it breached its SLA or not.

PETRA uses this history to learn patterns like:
- "Report Issue tickets assigned to TRMS FUNCTIONAL ENDUR have a 65% historical breach rate"
- "Tickets reported on a Friday afternoon are more likely to breach than Monday morning tickets"
- "S1 tickets from the IMOS application breach less often than S1 tickets from ENDUR"

When a new open ticket comes in, PETRA looks at all these factors and produces a score from 0–100% representing how likely that ticket is to breach.

On top of the model score, PETRA adds an **urgency boost** for open tickets that are already running late:
- Ticket is >50% through its SLA window → +8%
- Ticket is >75% through its SLA window → +15%
- Ticket has already passed its SLA target → +25%

**Important — what the model does NOT use:**
The model deliberately does NOT use how many hours a ticket has already been open when training. This is to prevent a statistical trap called "data leakage" — if you trained on elapsed time, the model would just be learning "old tickets breach more" which is obvious and useless for prediction. Instead, it only uses information that was known at the moment the ticket was created.

**The three risk tiers:**

| Tier | Condition | Action |
|---|---|---|
| Already Breached | Severity = S4+ (Breach) | Escalate immediately |
| AT RISK | Breach risk ≥ 70% AND not yet breached | Act now — still saveable |
| Monitor | Breach risk 40–69% | Review daily |
| Low Risk | Breach risk < 40% | On track |

**Zombie tickets:**
A zombie is any open ticket that has been open for more than twice its SLA target but has not yet been formally marked as S4+ (Breach). These are tickets that have been forgotten or stuck. PETRA flags them separately so they can be chased.

**Predicted breach datetime:**
For every open ticket, PETRA estimates the calendar date and time when it will breach its SLA, based on when it was reported and its SLA target. This turns the abstract "70% risk" into a concrete "this ticket will breach at 3:45 PM on Thursday."

**What it produces:**
- `PETRA_Master_Report.xlsx` — full data with all scores, plus Category Summary, Monthly Trend, Weekly Health Summary, Data Quality, and Model Info sheets
- `PETRA_Teams_Alert_YYYY-MM-DD.xlsx` — action-focused report with AT RISK, Already Breached, Monitor, Zombie, and By Team sheets
- `petra_breach_model.pkl` — the saved ML model (reused on next run if data hasn't changed significantly)
- `petra_charts.png` — summary charts

**If you want to change the risk thresholds (e.g. AT RISK at 60% instead of 70%):**
Find this section in `main.py`:
```python
df_at_risk = df_alert[
    (df_alert["Breach Risk %"] >= 70) &  ← change this number
    ...
]
df_monitor = df_alert[
    (df_alert["Breach Risk %"] >= 40) &  ← and this one
    ...
]
```

**If you want to change the zombie multiplier (e.g. flag at 1.5× instead of 2×):**
Find this line:
```python
ZOMBIE_MULTIPLIER = 2.0
```
Change `2.0` to your preferred multiplier.

**If you want to add more features to the model:**
Find the `FEATURES` list in `main.py`. You can add any numeric column that exists in the merged dataframe. Make sure the column is available at ticket creation time — do not add anything that is only known after the ticket is resolved.

**If the model AUC drops below 0.65:**
This means the model is not performing well — likely because the data distribution has shifted. Re-run with fresh data. If it stays low, consider retraining from scratch by deleting `petra_breach_model.pkl` and running `main.py` again.

---

### `dashboard.py` — Interactive HTML Dashboard

**What it does:**
Reads the Excel reports produced by `main.py` and generates a single self-contained HTML file that you can open in any browser — no internet connection required, no installation needed.

**Tabs in the dashboard:**
- **Overview** — KPI cards (AT RISK count, Breached, Monitor, Zombie), top 5 AT RISK tickets, risk distribution donut chart, breach rate by category
- **AT RISK** — searchable table of all AT RISK tickets
- **Already Breached** — searchable table of all breached tickets
- **Monitor** — medium-risk tickets
- **Zombies** — forgotten tickets open past 2× their SLA
- **By Category** — historical breach rate bar chart per issue type
- **By Team** — team performance chart and table
- **Trends** — monthly ticket volume and breach rate over time

**What it produces:**
`PETRA_Dashboard_YYYY-MM-DD.html` — opens automatically in your default browser after `main.py` finishes.

**If you want to add a new chart or tab:**
The dashboard is built in plain HTML + JavaScript inside `dashboard.py`. Each tab follows the same pattern. To add a new tab, add a button to the `<nav>` section, add a `<div id="tab-yourname">` panel in the content section, and wire it up in the `init()` JavaScript function.

---

## How to Run PETRA

### Prerequisites
Install the required Python packages once:
```
pip install pandas numpy openpyxl scikit-learn matplotlib tqdm
```

### Daily run
```
python main.py
```
This runs the full pipeline: SLA → Classifier → ML → Reports → Dashboard.

### Run individual steps
```
python sla.py          # SLA calculation only
python classifier.py   # Classification only
python dashboard.py    # Regenerate dashboard from existing reports
```

### Output location
All files are saved to:
```
C:\Users\[you]\OneDrive - PETRONAS\Desktop\PETRA Output\
```

---

## Input Files Required

| File | Location | Purpose |
|---|---|---|
| `Incident Raw Data - DD Mon YYYY.xlsx` | `PETRONAS\TRMS Internal - myGenie+ Extract\` | Daily incident extract from myGenie+ |
| `Holiday.csv` | `PETRONAS\PETCO Trading Digital - myGenie Ticket Analysis\Others Ref\` | Malaysian public holidays |

PETRA automatically picks the latest dated incident file — you do not need to rename or move anything.

---

## Common Issues & Fixes

| Problem | Likely cause | Fix |
|---|---|---|
| `FileNotFoundError: INCIDENT_ROOT not found` | The shared drive path has changed | Update `INCIDENT_ROOT` at the top of `sla.py` |
| `Holiday CSV is missing required columns` | Holiday.csv format changed | Ensure columns are named `Year`, `Month`, `Day` |
| `Could not find 'Incident ID' column` | Column name changed in myGenie+ extract | Check the actual column name and update `find_id_col()` in `main.py` |
| Model AUC shows N/A | Fewer than 10 closed tickets found | Check that the SLA file has closed tickets with a resolved date |
| Others category > 20% | Many tickets don't match any pattern | Review a sample of Others tickets and add new patterns to `classifier.py` |
| Dashboard shows blank charts | `main.py` hasn't been run yet today | Run `main.py` first, then `dashboard.py` |

---

## Understanding the Output Columns

| Column | Meaning |
|---|---|
| `Resolution_WorkingHours` | Hours elapsed within the 09:00–midnight working window |
| `Severity` | S1–S4 or S4+ (Breach) based on working hours vs SLA threshold |
| `SLA_Met` | True = within SLA, False = breached |
| `Refined Summary` | Issue category assigned by the classifier |
| `Confidence` | HIGH / MEDIUM / LOW — how sure the classifier is |
| `breach_risk_%` | 0–100% probability of SLA breach (ML model + urgency boost) |
| `risk_level` | 🔴 HIGH / 🟡 MEDIUM / 🟢 LOW |
| `predicted_breach_dt` | Estimated datetime when the ticket will breach its SLA |
| `hours_until_breach` | Hours remaining before predicted breach (negative = already past) |
| `is_zombie` | 1 = ticket is open >2× its SLA target age |

---

## Who Maintains This

Built by the Petronas Trading Digital Service Management team.
For questions about the code, contact the team lead or refer to the inline comments in each script.


---

## Sharing the Dashboard & Deployment Options

The dashboard is a single `.html` file. It contains all the data baked in — no server, no database, no login needed. Here are your options from simplest to most powerful.

---

### Option 1 — Share the HTML file directly (simplest, zero setup)

After running `main.py`, send the file `PETRA_Dashboard_YYYY-MM-DD.html` to anyone on the team via Teams, email, or SharePoint. They open it in any browser — Chrome, Edge, Firefox. No installation required.

Limitation: they need to receive a new file each day.

---

### Option 2 — Put it on SharePoint (recommended for Petronas)

1. Upload `PETRA_Dashboard_YYYY-MM-DD.html` to a SharePoint document library
2. Right-click → Share → copy the link
3. Anyone with the link can open it in their browser

To make it always show the latest version, save the file with a fixed name instead of a date:

Open `dashboard.py` and change this line:
```python
out_path = OUT_DIR / f"PETRA_Dashboard_{datetime.now().strftime('%Y-%m-%d')}.html"
```
to:
```python
out_path = OUT_DIR / "PETRA_Dashboard_Latest.html"
```

Then set up a scheduled task (see below) to run `main.py` every morning. The SharePoint link stays the same — it always shows today's data.

---

### Option 3 — Host on GitHub Pages (free public website)

This turns the dashboard into a real website anyone can visit with a URL like `https://yulin1709.github.io/PETRA-3.0/`.

**Steps:**

1. After running `main.py`, copy the generated HTML file into your repo and rename it `index.html`:
   ```
   copy "C:\Users\yulin.yeap\OneDrive - PETRONAS\Desktop\PETRA Output\PETRA_Dashboard_Latest.html" index.html
   ```

2. Commit and push:
   ```
   git add index.html
   git commit -m "Update dashboard"
   git push
   ```

3. In your GitHub repo, go to **Settings → Pages → Source → Deploy from branch → main → / (root) → Save**

4. After a minute, your dashboard is live at `https://yulin1709.github.io/PETRA-3.0/`

**Important:** GitHub Pages is public. Do not use this if your ticket data is confidential. Use SharePoint instead for internal Petronas use.

---

### Option 4 — Deploy as a private web app on Azure Static Web Apps (most professional)

This gives you a private URL that only Petronas staff with a Microsoft account can access.

1. In the Azure Portal, create a **Static Web App** resource
2. Connect it to your GitHub repo (`yulin1709/PETRA-3.0`)
3. Set the build output to `/` (root)
4. Enable **Azure Active Directory** authentication in the Static Web App settings
5. Push `index.html` to the repo — Azure auto-deploys it

Result: a URL like `https://your-app.azurestaticapps.net` that requires Petronas Microsoft login. Only people in your tenant can access it.

Cost: Free tier is sufficient for this use case.

---

### Option 5 — Run as a scheduled task (fully automated daily refresh)

Set up Windows Task Scheduler to run PETRA every morning at 8:00 AM automatically.

1. Open **Task Scheduler** → Create Basic Task
2. Name: `PETRA Daily Run`
3. Trigger: Daily at 08:00
4. Action: Start a program
   - Program: `C:\Users\yulin.yeap\AppData\Local\Programs\Python\Python314\python.exe`
   - Arguments: `main.py`
   - Start in: `C:\Users\yulin.yeap\OneDrive - PETRONAS\Desktop\VSC Projects\PETRA`
5. Finish

Each morning PETRA runs automatically, generates fresh reports, and overwrites `PETRA_Dashboard_Latest.html`. If you've set up SharePoint or GitHub Pages, the team sees updated data without anyone doing anything manually.

---

### Dashboard Features (v3.0)

| Feature | How to use |
|---|---|
| Date filter | Set From/To dates to see only tickets reported in that range |
| Team filter | Select a team to see only their tickets across all tabs |
| Assignee filter | Filter to a specific person's tickets |
| Severity filter | Show only S1, S2, S3, or S4 tickets |
| Category filter | Filter to a specific issue type |
| Reset button | Clears all filters instantly |
| Column sort | Click any column header to sort ascending/descending |
| Search box | Type anything to search within the visible table |
| ⬇ CSV button | Downloads the current filtered view as a CSV file (opens in Excel) |
| Predicted Breach column | Shows the estimated date/time the ticket will breach its SLA |
| Zombie tab | Lists tickets open more than 2× their SLA target |
| KPI cards | Update dynamically when filters are applied |
