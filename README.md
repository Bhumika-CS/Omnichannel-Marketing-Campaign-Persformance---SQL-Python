# Omnichannel-Marketing-Campaign-Persformance---SQL-Python
Workflow: Raw omnichannel data (third-party ads, vendor displays, internal emails) is transformed via SQL into a unified **Consolidated_Campaign_Data** table. A Python QC script then validates nulls, value ranges, and consistency, ensuring clean, integrated marketing data for analysis.

# Omnichannel Campaign Data QC

This project contains SQL and Python code to process and validate omnichannel HCP campaign data.

## Folder Structure
- `data/` - Input campaign data (CSV)
- `sql/` - SQL script to process the data
- `output/` - Output from the SQL job
- `scripts/` - Python script for QC validation
