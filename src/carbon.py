import json, os, math
from geopy.distance import geodesic

def load_aircraft_emissions(filepath=None):
    if filepath is None:
        filepath = os.path.join("base_data", "aircraft_emissions.json")
    with open(filepath, "r") as f:
        src = json.load(f)
    fc = {k: dict(v) for k, v in src.get("flight_categories", {}).items()}
    ac = {k: {kk: float(vv) for kk, vv in v.items()} for k, v in src.get("aircraft", {}).items()}
    return fc, ac

def load_train_emissions(filepath=None):
    if filepath is None:
        filepath = os.path.join("base_data", "train_emissions.json")
    with open(filepath, "r") as f:
        return json.load(f)

TRAIN_FACTORS = load_train_emissions()
FLIGHT_CATEGORIES, AIRCRAFT_CATEGORY_CO2 = load_aircraft_emissions()

EMISSION_FACTORS = {
    'bus': {'construction': 4.42, 'fuel': 25.0, 'infrastructure': 0.7},
    'car': {'construction': 25.6, 'fuel': 192.0, 'infrastructure': 0.7, 'additional_passenger_factor': 0.04},
    'air': {
        'short': {'base_co2_per_km': 0.300},
        'medium': {'base_co2_per_km': 0.200},
        'long': {'base_co2_per_km': 0.167},
        'non_co2_factor': 1.7,
        'detour_factor': 1.076,
    },
    'ferry': {'combustion': 80.0, 'services': 30.0, 'construction': 11.0},
    'cycle': {'construction': 5.0, 'human_fuel': 16.0},
    'walk': {'human_fuel': 16.0},
    'metro': 'train',
    'tram': 'train',
    'aerialway': 'train'
}

def calculate_great_circle_distance(p1, p2):
    return geodesic(p1, p2).meters

def calculate_path_distance(path):
    if len(path) < 2: return 0
    return sum(calculate_great_circle_distance(path[i], path[i+1]) for i in range(len(path)-1))

def get_flight_category(distance_km, categories=FLIGHT_CATEGORIES):
    cand = []
    for name, b in categories.items():
        dmin = b.get("distance_km_min", float("-inf"))
        dmax = b.get("distance_km_max", float("inf"))
        if dmin <= distance_km < dmax:
            span = (dmax - dmin) if math.isfinite(dmax) and math.isfinite(dmin) else float("inf")
            cand.append((span, dmin, name))
    return min(cand)[2] if cand else None

def get_aircraft_co2_value(aircraft_code, distance_km):
    per_cat = AIRCRAFT_CATEGORY_CO2.get(aircraft_code)
    if not per_cat: return None
    cat = get_flight_category(distance_km)
    return per_cat.get(cat) if cat and cat in per_cat else per_cat.get("all")

def get_trip_distance_km(trip, path, trip_type):
    if trip_type == 'air' and len(path) == 2:
        m = calculate_great_circle_distance(path[0], path[1])
        return (m/1000) * EMISSION_FACTORS['air']['detour_factor']
    if trip_type == 'air' and len(path) > 2:
        return calculate_path_distance(path) / 1000
    m = trip.get('trip_length', 0) or (calculate_path_distance(path) if path else 0)
    return m / 1000

def calculate_air_emissions(distance_km, path_points, aircraft_code=''):
    f = EMISSION_FACTORS['air']
    v = get_aircraft_co2_value(aircraft_code, distance_km)
    if v is not None:
        return distance_km * v * f['non_co2_factor']
    cat = 'short' if distance_km < 1000 else ('medium' if distance_km < 3500 else 'long')
    return distance_km * f[cat]['base_co2_per_km'] * f['non_co2_factor']

def _split_km_for_country(cc, value_m):
    if isinstance(value_m, dict):
        e_km = (value_m.get('electric_m', 0) or 0) / 1000
        d_km = (value_m.get('diesel_m', 0) or 0) / 1000
        return e_km, d_km
    total_km = (value_m or 0) / 1000
    diesel_share = TRAIN_FACTORS.get(cc, TRAIN_FACTORS['default']).get('diesel_share', TRAIN_FACTORS['default']['diesel_share'])
    d_km = total_km * diesel_share
    return total_km - d_km, d_km

def calculate_train_emissions(distance_km, countries, force_electric=False):
    if not countries:
        g = TRAIN_FACTORS['default']
        return distance_km * (g['infrastructure'] + g['manufacturing'] + g['electric_upstream']) / 1000
    if isinstance(countries, str):
        try: countries = json.loads(countries)
        except: countries = {}

    total = 0.0
    for cc, val in countries.items():
        g = TRAIN_FACTORS.get(cc, TRAIN_FACTORS['default'])
        e_km, d_km = _split_km_for_country(cc, val)
        if force_electric:
            e_km, d_km = e_km + d_km, 0.0
        km = e_km + d_km
        total += km * g['infrastructure'] + km * g['manufacturing']
        total += e_km * g['electric_upstream'] + d_km * g['diesel_fuel']
    return total / 1000

def calculate_bus_emissions(distance_km):
    g = EMISSION_FACTORS['bus']
    return distance_km * (g['construction'] + g['fuel'] + g['infrastructure']) / 1000

def calculate_car_emissions(distance_km, passengers=1):
    g = EMISSION_FACTORS['car']
    total = distance_km * (g['construction'] + g['fuel'] + g['infrastructure'])
    if passengers > 1:
        total += distance_km * g['fuel'] * g['additional_passenger_factor'] * (passengers - 1)
    return (total / passengers) / 1000

def calculate_ferry_emissions(distance_km):
    g = EMISSION_FACTORS['ferry']
    return distance_km * (g['combustion'] + g['services'] + g['construction']) / 1000

def calculate_cycle_emissions(distance_km):
    g = EMISSION_FACTORS['cycle']
    return distance_km * (g['construction'] + g['human_fuel']) / 1000

def calculate_walk_emissions(distance_km):
    g = EMISSION_FACTORS['walk']
    return distance_km * g['human_fuel'] / 1000

def calculate_carbon_footprint_for_trip(trip, path):
    t = trip.get('type', '').lower()
    if t not in ['train','bus','air','helicopter','ferry','cycle','walk','metro','tram','aerialway','car']:
        return 0
    if t == 'helicopter': t = 'air'
    distance_km = get_trip_distance_km(trip, path, t)
    if distance_km == 0: return 0
    if t == 'air':
        return calculate_air_emissions(distance_km, len(path), trip.get('material_type',''))
    if t in ['train']:
        return calculate_train_emissions(distance_km, trip.get('countries', {}), force_electric=False)
    if t in ['metro','tram','aerialway']:
        return calculate_train_emissions(distance_km, trip.get('countries', {}), force_electric=True)
    if t == 'bus':
        return calculate_bus_emissions(distance_km)
    if t == 'car':
        return calculate_car_emissions(distance_km, trip.get('passengers', 1))
    if t == 'ferry':
        return calculate_ferry_emissions(distance_km)
    if t == 'cycle':
        return calculate_cycle_emissions(distance_km)
    if t == 'walk':
        return calculate_walk_emissions(distance_km)
    return 0