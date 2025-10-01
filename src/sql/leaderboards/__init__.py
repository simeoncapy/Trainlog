from src.sql import SqlTemplate

# Leaderboard queries
leaderboard_stats = SqlTemplate("src/sql/leaderboards/leaderboard_stats.sql")
countries_leaderboard = SqlTemplate("src/sql/leaderboards/countries_leaderboard.sql")
