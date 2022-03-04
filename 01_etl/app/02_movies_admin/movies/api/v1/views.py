from django.contrib.postgres.aggregates.general import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView
from movies.models import Filmwork

FILMWORK_FIELDS_OF_INTEREST = [
    "id",
    "title",
    "description",
    "creation_date",
    "rating",
    "type",
]


def get_person_names_aggregation(role: str):
    return ArrayAgg(
        "persons__full_name",
        filter=Q(personfilmwork__role=role),
        distinct=True,
    )


class MoviesApiMixin:
    model = Filmwork
    http_method_names = ["get"]

    def get_queryset(self):
        out = Filmwork.objects.all()
        out = out.values(*FILMWORK_FIELDS_OF_INTEREST)
        out = out.annotate(
            genres=ArrayAgg("genres__name", distinct=True),
            actors=get_person_names_aggregation(role="actor"),
            writers=get_person_names_aggregation(role="writer"),
            directors=get_person_names_aggregation(role="director"),
        )

        return out  # Сформированный QuerySet

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):
    paginate_by = 50

    def get_page_number(self, total_pages: int) -> int:
        page_n = self.request.GET.get("page") or "1"
        if isinstance(page_n, str):
            if page_n.isnumeric():
                page_n = int(page_n)
            elif page_n == "last":
                page_n = total_pages
            elif page_n == "first":
                page_n = 1
            else:
                raise KeyError("Bad page")

        return page_n

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = self.get_queryset()
        # this paginator badly processes page number, so must take it by hand(
        paginator, _, queryset, is_paginated = self.paginate_queryset(
            queryset=queryset,
            page_size=self.paginate_by,
        )
        page_n = self.get_page_number(total_pages=paginator.num_pages)
        p = paginator.get_page(page_n)

        return {
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            "prev": p.previous_page_number() if p.has_previous() else None,
            "next": p.next_page_number() if p.has_next() else None,
            "results": list(p.object_list),
        }


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):
    def get_context_data(self, **kwargs):
        return super(MoviesDetailApi, self).get_context_data(**kwargs)[
            "object"
        ]
