from src.sql import SqlTemplate

get_admin_areas_by_level = SqlTemplate("src/sql/completion/get_admin_areas_by_level.sql")
get_coverage_units = SqlTemplate("src/sql/completion/get_coverage_units.sql")
delete_coverage_units = SqlTemplate("src/sql/completion/delete_coverage_units.sql")
merge_coverage_units = SqlTemplate("src/sql/completion/merge_coverage_units.sql")
update_admin_area_geom = SqlTemplate("src/sql/completion/update_admin_area_geom.sql")
