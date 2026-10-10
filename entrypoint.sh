#!/bin/sh
echo "$CRON_SCHEDULE /app/scheduled-run.sh" > /app/crontab
/app/scheduled-run.sh
exec supercronic /app/crontab