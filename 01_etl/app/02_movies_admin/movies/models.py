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

    # auto_now_add автоматически выставит дату создания записи
    created = models.DateTimeField(_("created"), auto_now_add=True)
    # auto_now изменятся при каждом обновлении записи
    modified = models.DateTimeField(_("modified"), auto_now=True)

    class Meta(object):
        """
        Этот параметр указывает Django.

        что этот класс не является представлением таблицы
        """

        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta(object):
        abstract = True


class Genre(TimeStampedMixin, UUIDMixin):
    """
    Типичная модель в Django использует число в качестве id.

    В таких ситуациях поле не описывается в модели.
    Вам же придётся явно объявить primary key.
    """

    # Первым аргументом обычно идёт человеко-читаемое название поля
    name = models.CharField(_("title"), max_length=GENRE_NAME_LEN)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField(_("description"), blank=True, null=True)

    class Meta(object):
        """
        Ваши таблицы находятся в нестандартной схеме.

        Это нужно указать в классе модели
        """

        db_table = 'content"."genre'
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = "жанр"
        verbose_name_plural = "жанры"
        constraints = [
            models.UniqueConstraint(fields=["name"], name="uniq genre name")
        ]

    def __str__(self):
        return self.name


class Person(TimeStampedMixin, UUIDMixin):
    full_name = models.TextField(_("full name"), blank=True)
    birth_date = models.DateField(_("birth_date"), blank=True, null=True)

    class Meta(object):
        db_table = 'content"."person'
        verbose_name = "артист"
        verbose_name_plural = "артисты"

    def __str__(self):
        return self.full_name


class Filmwork(TimeStampedMixin, UUIDMixin):
    """
    Типичная модель в Django использует число в качестве id.

    В таких ситуациях поле не описывается в модели.
    Вам же придётся явно объявить primary key.
    """

    certificate = models.TextField(_("certificate"), blank=True, null=True)
    file_path = models.TextField(_("file_path"), blank=True, null=True)
    title = models.TextField(_("title"), blank=True)
    description = models.TextField(_("description"), blank=True)
    creation_date = models.DateTimeField(_("creation_date"))
    rating = models.FloatField(
        _("rating"),
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
    )
    type = models.TextField(_("type"), choices=TypesOfFilmwork.choices)
    genres = models.ManyToManyField(Genre, through="GenreFilmwork")
    persons = models.ManyToManyField(Person, through="PersonFilmwork")

    class Meta(object):
        """
        Ваши таблицы находятся в нестандартной схеме.

        Это нужно указать в классе модели
        """

        db_table = 'content"."film_work'
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = "Кинопроизведение"
        verbose_name_plural = "Кинопроизведения"

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    """ORM for table Genre-Filmwork.

    provides many-to-many realtion
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
        verbose_name = "жанр"
        verbose_name_plural = "жанры"


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
        verbose_name = "Артист в кинопроизведении"
        verbose_name_plural = "Артисты в кинопроизведении"
        constraints = [
            models.UniqueConstraint(
                fields=["role", "person_id", "film_work_id"],
                name="uniq role-person-film name",
            ),
        ]
