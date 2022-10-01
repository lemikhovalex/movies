#!/bin/bash

# Start the first process

/wait \
    && python3 manage.py collectstatic --noinput \
    && python3 manage.py migrate \

python3 manage.py createsuperuser --noinput || echo "SU creation failed"

gunicorn config.wsgi:application --bind backend:80 --workers 1 --reload


# Wait for any process to exit
wait -n
  
# Exit with status of process that exited first
exit $?