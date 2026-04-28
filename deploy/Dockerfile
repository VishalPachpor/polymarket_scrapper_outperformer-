FROM python:3.12-slim

WORKDIR /app

# copy-trader uses stdlib only — no pip deps needed
COPY tip_v1/ ./tip_v1/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TIP_DB_PATH=/data/copy_trader.sqlite3

CMD ["python", "-m", "tip_v1.cli.run_copy_trader", "--log-level", "INFO"]
