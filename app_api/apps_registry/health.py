"""Liveness / readiness probe used by the container healthcheck."""

from django.db import connection
from django.http import JsonResponse


def healthz(_request):
    """Return 200 when Postgres accepts a connection, else 503.

    Compose and bring_up wait on this before pointing the crawler at the
    registry. A cheap SELECT 1 is enough: if the ORM can talk to the DB,
    migrations either already ran or will fail loudly on the next write.
    """
    try:
        connection.ensure_connection()
        with connection.cursor() as cursor:
            cursor.execute('SELECT 1')
    except Exception:
        return JsonResponse({'status': 'unavailable'}, status=503)
    return JsonResponse({'status': 'ok'})
