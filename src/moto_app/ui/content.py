from __future__ import annotations


APP_TITLE = "Moto Weekly Operator"

INSTRUCTIONS_TEXT = """
Weekly run
1. Export the latest Platforma Opon CSV and place it in the data folder or select it in the app.
2. Confirm the file date matches the intended weekly snapshot.
3. Use "Refresh reference data" only when campaign or mapping workbooks changed.
4. Use "Replace snapshot" only when rerunning the same week after a backend or data fix.
5. Start the run and wait for the final status before opening reports.

Troubleshooting
- If the CSV is missing columns, export the source file again from Platforma Opon.
- If a snapshot already exists, rerun only when you intentionally want to replace that week.
- If report generation fails, check the latest run log first.
- If reference refresh fails, verify the workbook names and sheet structure in data/campaign rules.
""".strip()
