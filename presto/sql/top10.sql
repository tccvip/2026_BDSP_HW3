SELECT movie_id, title, rating_count, avg_rating
FROM mongodb.bdsp_hw3.movie_stats
WHERE rating_count >= 20
ORDER BY avg_rating DESC, rating_count DESC, movie_id ASC
LIMIT 10;
