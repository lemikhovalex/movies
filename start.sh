#!/bin/bash

# Start the first process

/wait \
    && python manage.py collectstatic --noinput \
    && python manage.py migrate \

python manage.py createsuperuser --noinput || echo "SU creation failed"

gunicorn config.wsgi:application --bind admin:8000 --workers 1 --reload


# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?