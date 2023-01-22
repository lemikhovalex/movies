"""
Apps file in Django structure.

for now dunno what is it for
"""

from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class MoviesConfig(AppConfig):
    """class with configs for app. havn't used it for now."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "movies"
    verbose_name = _("movies")
