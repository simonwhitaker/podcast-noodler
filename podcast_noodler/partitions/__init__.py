from dagster import MonthlyPartitionsDefinition, WeeklyPartitionsDefinition

weekly_partition = WeeklyPartitionsDefinition(start_date="2024-03-01")
