from django.db import models
from django.utils.translation import gettext_lazy as _


class TypesOfFilmwork(models.TextChoices):
    MOVIE = "MOVIE", _("type_movie")
    TV_SHOW = "TV_SHOW", _("type_tv_show")


class Roles(models.TextChoices):
    WRITER = "WRITER", _("Writer")
    ACTOR = "ACTOR", _("Actor")
    DIRECTOR = "DIRECTOR", _("Director")
