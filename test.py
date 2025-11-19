
from orchestrator.reports.route_status_report import generate_daily_completeness_report

#df_today = generate_daily_completeness_report("2025-11-19")

#print(df_today)

df_today = generate_daily_completeness_report("2025-11-01", "2025-11-28")

print(df_today)