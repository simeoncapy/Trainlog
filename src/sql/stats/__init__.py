from pathlib import Path
from src.sql import SqlTemplate

# Load CTE templates
CTE_DIR = Path(__file__).parent / "cte"
CTES = {}

for cte_file in CTE_DIR.glob("*.sql"):
    cte_name = cte_file.stem
    with open(cte_file, 'r') as f:
        content = f.read()
        # Wrap in CTE format
        CTES[cte_name] = f", {cte_name} AS (\n{content}\n)"

# Base filter is always first (no comma prefix)
if 'base_filter' in CTES:
    CTES['base_filter'] = f"WITH base_filter AS (\n{open(CTE_DIR / 'base_filter.sql').read()}\n)"


class ComposedSqlTemplate:
    """SQL template that composes CTEs dynamically"""
    
    def __init__(self, query_file, required_ctes=None):
        self.query_file = Path(query_file)
        self.required_ctes = required_ctes or []
        
        with open(self.query_file, 'r') as f:
            self.query_template = f.read()
    
    def __call__(self):
        """Build the complete query with required CTEs"""
        from sqlalchemy import text
        
        # Build CTE chain
        cte_sql = ""
        for cte_name in self.required_ctes:
            if cte_name in CTES:
                cte_sql += CTES[cte_name]
        
        # Replace placeholders in query template
        query = self.query_template
        for cte_name in self.required_ctes:
            query = query.replace(f"{{{cte_name}}}", "")
        
        # Combine CTEs with query
        full_query = cte_sql + "\n\n" + query
        
        return text(full_query)


# Define which CTEs each query needs
stats_operator_km = ComposedSqlTemplate(
    "src/sql/stats/stats_operator_km.sql",
    ['base_filter', 'time_categories', 'split_operators']
)

stats_operator_trips = ComposedSqlTemplate(
    "src/sql/stats/stats_operator_trips.sql",
    ['base_filter', 'time_categories', 'split_operators']
)

stats_material_km = ComposedSqlTemplate(
    "src/sql/stats/stats_material_km.sql",
    ['base_filter', 'time_categories', 'split_material']
)

stats_material_trips = ComposedSqlTemplate(
    "src/sql/stats/stats_material_trips.sql",
    ['base_filter', 'time_categories', 'split_material']
)

stats_countries = ComposedSqlTemplate(
    "src/sql/stats/stats_countries.sql",
    ['base_filter', 'time_categories']
)

stats_routes_km = ComposedSqlTemplate(
    "src/sql/stats/stats_routes_km.sql",
    ['base_filter', 'time_categories']
)

stats_routes_trips = ComposedSqlTemplate(
    "src/sql/stats/stats_routes_trips.sql",
    ['base_filter', 'time_categories']
)

stats_stations_km = ComposedSqlTemplate(
    "src/sql/stats/stats_stations_km.sql",
    ['base_filter', 'time_categories']
)

stats_stations_trips = ComposedSqlTemplate(
    "src/sql/stats/stats_stations_trips.sql",
    ['base_filter', 'time_categories']
)

stats_year_km = ComposedSqlTemplate(
    "src/sql/stats/stats_year_km.sql",
    ['base_filter', 'time_categories']
)

stats_year_trips = ComposedSqlTemplate(
    "src/sql/stats/stats_year_trips.sql",
    ['base_filter', 'time_categories']
)

# Simple queries without CTEs
type_available = SqlTemplate("src/sql/stats/type_available.sql")
distinct_stat_years = SqlTemplate("src/sql/stats/distinct_stat_years.sql")
public_stats = SqlTemplate("src/sql/stats/public_stats.sql")
