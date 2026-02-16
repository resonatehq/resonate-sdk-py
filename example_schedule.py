"""Example demonstrating the high-level schedule API."""

from resonate import Context, Resonate

# Create a local Resonate instance
resonate = Resonate.local()


# Define a function to schedule
@resonate.register
def generate_report(ctx: Context, user_id: int, report_type: str) -> str:
    """Generate a report for a user."""
    return f"Generated {report_type} report for user {user_id}"


# Example 1: Schedule using the Resonate class
print("Example 1: Schedule using resonate.schedule()")
schedule1 = resonate.schedule(
    "daily_report_schedule",
    generate_report,
    "0 9 * * *",  # Every day at 9am
    user_id=123,
    report_type="daily",
)
print(f"Created schedule: {schedule1.id}")
print(f"Cron expression: {schedule1.cron}")
print(f"Promise ID pattern: {schedule1.promise_id}")
print()

# Example 2: Schedule using the Function class
print("Example 2: Schedule using function.schedule()")
schedule2 = generate_report.schedule(
    "weekly_report_schedule",
    "0 9 * * 1",  # Every Monday at 9am
    user_id=456,
    report_type="weekly",
)
print(f"Created schedule: {schedule2.id}")
print(f"Cron expression: {schedule2.cron}")
print()

# Example 3: Schedule with options (timeout and tags)
print("Example 3: Schedule with custom options")
schedule3 = resonate.options(
    timeout=3600,
    tags={"env": "production", "priority": "high"},
).schedule(
    "priority_report_schedule",
    generate_report,
    "*/30 * * * *",  # Every 30 minutes
    user_id=789,
    report_type="realtime",
)
print(f"Created schedule: {schedule3.id}")
print(f"Promise tags: {schedule3.promise_tags}")
print()

# Example 4: Schedule with function options
print("Example 4: Schedule with function-level options")
schedule4 = generate_report.options(
    timeout=7200,
    tags={"team": "analytics"},
).schedule(
    "analytics_report_schedule",
    "0 0 * * *",  # Every day at midnight
    user_id=999,
    report_type="analytics",
)
print(f"Created schedule: {schedule4.id}")
print()

# Cleanup
print("Cleaning up schedules...")
schedule1.delete()
schedule2.delete()
schedule3.delete()
schedule4.delete()
print("Done!")
