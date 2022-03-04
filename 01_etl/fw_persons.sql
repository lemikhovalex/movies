SELECT
	"content"."film_work"."id",
	"content"."person"."full_name",
	"content"."person"."id",
	"content"."person_film_work"."role"
FROM 
	"content"."film_work"
	LEFT OUTER JOIN "content"."person_film_work" ON ("content"."film_work"."id" = "content"."person_film_work"."film_work_id")
	LEFT OUTER JOIN "content"."person" ON ("content"."person_film_work"."person_id" = "content"."person"."id")
WHERE "content"."film_work"."id" = '68dfb5e2-7014-4738-a2da-c65bd41f5af5'