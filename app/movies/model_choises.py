from django.db import models
from django.utils.translation import gettext_lazy as _


class TypesOfFilmwork(models.TextChoices):
    MOVIE = "movie", _("type_movie")
    TV_SHOW = "tv_show", _("type_tv_show")


class Roles(models.TextChoices):
    WRITER = "writer", _("Writer")
    ACTOR = "actor", _("Actor")
    DIRECTOR = "director", _("Director")
