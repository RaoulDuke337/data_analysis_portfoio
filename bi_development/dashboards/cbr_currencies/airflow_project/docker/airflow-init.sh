#!/usr/bin/env bash
set -e

echo "⏳ init db Airflow..."
airflow db upgrade

echo "🔍 check admin user existing"

if airflow users list | grep -q 'admin'; then
  echo "✅ user 'admin' already exists. Skipping creation."
else
  echo "👤 create a user 'admin'..."
  airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
    --firstname "Admin" \
    --lastname "User" \
    --role "Admin" \
    --email "${_AIRFLOW_WWW_USER_EMAIL}"
fi

echo "✅ init complete."
