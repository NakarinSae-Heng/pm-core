#!/bin/bash
PIDS=$(pgrep -f "gunicorn -w 2 -b 0.0.0.0:5000 app:app")
if [[ -z "$PIDS" ]]; then
  echo "❌ PM Core is not running"
else
  echo "✅ PM Core is running (PID(s): $PIDS)"
fi
