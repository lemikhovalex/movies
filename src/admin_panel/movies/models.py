"""ORM's list for django.

keep code tidy, follow flake8
"""

import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _

from .model_choises import Roles, TypesOfFilmwork

GENRE_NAME_LEN = 255


class TimeStampedMixin(models.Model):
    """
    Here is class for adding time step features.

    created and modified
    """

    # auto_now_add automatically on creation
    created = models.DateTimeField(_("created"), auto_now_add=True)
    # auto_now automatically on modification
    modified = models.DateTimeField(_("modified"), auto_now=True)

    class Meta(object):
        """
        Make class abstract, not a real table in DB
        """

        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta(object):
        abstract = True


class Genre(TimeStampedMixin, UUIDMixin):

    name = models.CharField(_("title"), max_length=GENRE_NAME_LEN)
    description = models.TextField(_("description"), blank=True, null=True)

    class Meta(object):
        """
        specify db_table because it is not in a standard scheme
        """

        db_table = 'content"."genre'
        verbose_name = _("genre")
        verbose_name_plural = _("genres")
        constraints = [models.UniqueConstraint(fields=["name"], name="uniq genre name")]

    def __str__(self):
        return self.name


class Person(TimeStampedMixin, UUIDMixin):
    full_name = models.TextField(_("full name"), blank=True)
    birth_date = models.DateField(_("birth_date"), blank=True, null=True)

    class Meta(object):
        db_table = 'content"."person'
        verbose_name = _("person")
        verbose_name_plural = _("persons")

    def __str__(self):
        return self.full_name


class Filmwork(TimeStampedMixin, UUIDMixin):

    certificate = models.TextField(_("certificate"), blank=True, null=True)
    file_path = models.TextField(_("file_path"), blank=True, null=True)
    title = models.TextField(_("title"), blank=True)
    description = models.TextField(_("description"), blank=True, null=True)
    creation_date = models.DateTimeField(_("creation_date"), null=True)
    rating = models.FloatField(
        _("rating"),
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        null=True,
    )
    type = models.TextField(_("type"), choices=TypesOfFilmwork.choices)
    genres = models.ManyToManyField(Genre, through="GenreFilmwork")
    persons = models.ManyToManyField(Person, through="PersonFilmwork")

    class Meta(object):

        db_table = 'content"."film_work'
        verbose_name = _("film-work")
        verbose_name_plural = _("film-works")

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    """ORM for table Genre-Filmwork.

    provides many-to-many relation
    """

    film_work = models.ForeignKey(
        "Filmwork",
        on_delete=models.CASCADE,
        verbose_name=_("film_work"),
    )
    genre = models.ForeignKey(
        "Genre",
        on_delete=models.CASCADE,
        verbose_name=_("film_work"),
    )
    created = models.DateTimeField(auto_now_add=True)

    class Meta(object):
        db_table = 'content"."genre_film_work'
        verbose_name = _("genre-of-a-film-work")
        verbose_name_plural = _("genres-of-a-film-work")


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey(
        "Filmwork",
        on_delete=models.CASCADE,
        verbose_name=_("film_work"),
    )
    person = models.ForeignKey(
        "Person",
        on_delete=models.CASCADE,
        verbose_name=_("artist"),
    )
    role = models.TextField(
        _("role"),
        null=True,
        choices=Roles.choices,
    )
    created = models.DateTimeField(_("created"), auto_now_add=True)

    class Meta(object):
        db_table = 'content"."person_film_work'
        verbose_name = _("person-of-a-film-work")
        verbose_name_plural = _("persons-of-a-film-work")
        constraints = [
            models.UniqueConstraint(
                fields=["role", "person_id", "film_work_id"],
                name="uniq role-person-film name",
            ),
        ]
