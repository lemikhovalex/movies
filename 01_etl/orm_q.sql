SELECT
	"content"."film_work"."id",
	"content"."film_work"."title",
	"content"."film_work"."description",
	"content"."film_work"."creation_date",
	"content"."film_work"."rating",
	"content"."film_work"."type",
	ARRAY_AGG(DISTINCT "content"."genre"."name" ) AS "genres",
	ARRAY_AGG(DISTINCT "content"."person"."full_name" ) FILTER (WHERE T6."role" = 'actor') AS "actors",
	ARRAY_AGG(DISTINCT "content"."person"."full_name" ) FILTER (WHERE T6."role" = 'writer') AS "writers",
	ARRAY_AGG(DISTINCT "content"."person"."full_name" ) FILTER (WHERE T6."role" = 'director') AS "directors"
FROM 
	"content"."film_work"
	LEFT OUTER JOIN "content"."genre_film_work" ON ("content"."film_work"."id" = "content"."genre_film_work"."film_work_id")
	LEFT OUTER JOIN "content"."genre" ON ("content"."genre_film_work"."genre_id" = "content"."genre"."id")
	LEFT OUTER JOIN "content"."person_film_work" ON ("content"."film_work"."id" = "content"."person_film_work"."film_work_id")
	LEFT OUTER JOIN "content"."person" ON ("content"."person_film_work"."person_id" = "content"."person"."id")
	LEFT OUTER JOIN "content"."person_film_work" T6 ON ("content"."person"."id" = T6."person_id") 
WHERE "content"."film_work"."id" = '68dfb5e2-7014-4738-a2da-c65bd41f5af5'
GROUP BY "content"."film_work"."id"