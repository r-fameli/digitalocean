#!/bin/bash
set -e
uvicorn mock_inference:app --host 0.0.0.0 --port 8081 &
sleep 1
exec uvicorn server:app --host 0.0.0.0 --port ${PORT:-8080}
