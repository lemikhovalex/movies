# from django.contrib import admin

# Register your models here.
from django.contrib import admin

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    pass


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    pass


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline)

    # Отображение полей в списке
    list_display = ("title", "type", "creation_date", "rating")
    list_filter = ("type",)

    def item_count(self, obj):
        return obj.items.count()

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.prefetch_related("genres", "persons")
