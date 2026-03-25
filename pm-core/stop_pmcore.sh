#!/bin/bash
echo "🛑 Stopping PM Core"
pkill -f "gunicorn -w 2 -b 0.0.0.0:5000 app:app"
sleep 2
if pgrep -f "gunicorn -w 2 -b 0.0.0.0:5000 app:app" > /dev/null; then
  echo "⚠️  Force killing remaining processes..."
  pkill -9 -f "gunicorn -w 2 -b 0.0.0.0:5000 app:app"
fi
echo "✅ PM Core stopped"
