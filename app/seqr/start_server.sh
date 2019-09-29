cd /opt/seqr-local/seqr/seqr_settings

LOG_FILE=$(pwd)/gunicorn.log
nohup gunicorn -w 4 -c gunicorn_config.py wsgi:application --bind 0.0.0.0:8000 >& ${LOG_FILE} &
echo "gunicorn started in background. See ${LOG_FILE}"

