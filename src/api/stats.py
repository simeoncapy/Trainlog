import logging
from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify, abort

from src.pg import pg_session
from src.sql import stats as stats_sql
from src.utils import (
    getUser,
    isCurrentTrip,
    lang,
    listOperatorsLogos,
    get_user_id
)

logger = logging.getLogger(__name__)

stats_blueprint = Blueprint("stats", __name__)

import json


def get_stats_general(pg, query_func, user_id, stat_name, trip_type, year=None):
    """Generic stats fetcher for operators, material, routes, stations"""
    result = pg.execute(
        query_func(),
        {"user_id": user_id, "tripType": trip_type, "year": year}
    ).fetchall()
    
    stats = []
    for row in result:
        row_dict = dict(row._mapping)
        if row_dict.get(stat_name):
            stats.append(row_dict)
    return stats


def get_podiumized_stats(pg, query_func, user_id, stat_name, trip_type, year=None):
    """Get stats arranged for podium display (1st, 2nd, 3rd)"""
    raw_stats = get_stats_general(pg, query_func, user_id, stat_name, trip_type, year)
    
    for index, stat in enumerate(raw_stats):
        raw_stats[index]["height"] = len(raw_stats) - index

    stats = []
    if len(raw_stats) == 3:
        stats.append(raw_stats[1])  # 2nd place
        stats.append(raw_stats[0])  # 1st place
        stats.append(raw_stats[2])  # 3rd place
    return stats


def get_stats_countries(pg, query_func, user_id, km, trip_type, year=None):
    """Process country statistics with past/future breakdown"""
    result = pg.execute(
        query_func(),
        {"user_id": user_id, "tripType": trip_type, "year": year}
    ).fetchall()
    
    countries = {}
    
    for row in result:
        row_dict = dict(row._mapping)
        country_list = row_dict['countries']
        
        try:
            country_dict = json.loads(country_list)
        except (json.JSONDecodeError, TypeError):
            continue
        
        for country in country_dict:
            if country not in countries:
                countries[country] = {
                    "total": 0,
                    "past": 0,
                    "plannedFuture": 0
                }
            
            if isinstance(country_dict[country], dict):
                country_value = sum(country_dict[country].values())
            else:
                country_value = country_dict[country]
            
            if km:
                countries[country]["total"] += country_value
                if row_dict["past"] != 0:
                    countries[country]["past"] += country_value
                elif row_dict["plannedFuture"] != 0:
                    countries[country]["plannedFuture"] += country_value
            else:
                countries[country]["total"] += (
                    row_dict["past"] + row_dict["plannedFuture"]
                )
                if row_dict["past"] != 0:
                    countries[country]["past"] += row_dict["past"]
                elif row_dict["plannedFuture"] != 0:
                    countries[country]["plannedFuture"] += row_dict["plannedFuture"]
    
    # Sort by total descending
    countries = dict(
        sorted(
            countries.items(),
            key=lambda item: countries[item[0]]["total"],
            reverse=True,
        )
    )
    
    # Convert to list format
    countries_list = []
    for country in countries:
        countries_list.append(
            {
                "country": country,
                "past": countries[country]["past"],
                "plannedFuture": countries[country]["plannedFuture"],
            }
        )
    
    return countries_list


def get_stats_years(pg, query_func, user_id, lang, trip_type, year=None):
    """Process year statistics with gap filling"""
    years = []
    years_temp = {}
    future = None
    
    result = pg.execute(
        query_func(),
        {"user_id": user_id, "tripType": trip_type, "year": year}
    ).fetchall()

    if len(result) == 0:
        return ""
    
    result_list = [dict(row._mapping) for row in result]
    
    # Separate future from regular years
    future = next((y for y in result_list if y["year"] == "future"), None)
    result_list = [y for y in result_list if y["year"] != "future"]
    
    if len(result_list) == 0:
        if future:
            return [{
                "year": lang["future"],
                "past": 0,
                "plannedFuture": 0,
                "future": future["future"],
            }]
        return ""
    
    # Build temp dictionary
    for year_row in result_list:
        years_temp[int(year_row["year"])] = {
            "past": int(year_row["past"]),
            "plannedFuture": int(year_row["plannedFuture"]),
            "future": int(year_row["future"]),
        }
    
    # Fill gaps between first and last year
    first_year = int(result_list[0]["year"])
    last_year = int(result_list[-1]["year"])
    
    for year_num in range(first_year, last_year + 1):
        if year_num in years_temp:
            years.append(
                {
                    "year": year_num,
                    "past": years_temp[year_num]["past"],
                    "plannedFuture": years_temp[year_num]["plannedFuture"],
                    "future": years_temp[year_num]["future"],
                }
            )
        else:
            years.append({
                "year": year_num,
                "past": 0,
                "plannedFuture": 0,
                "future": 0
            })
    
    # Add future if exists
    if future:
        years.append(
            {
                "year": lang["future"],
                "past": 0,
                "plannedFuture": 0,
                "future": future["future"],
            }
        )
    
    return years


def fetch_stats(username, trip_type, year=None):
    """Fetch all statistics for a user and trip type"""
    stats = {}
    user_id = get_user_id(username)
    
    with pg_session() as pg:
        # Check if trip type is available for user
        available_types = pg.execute(
            stats_sql.type_available(),
            {"user_id": user_id}
        ).fetchall()
        
        type_exists = any(row[0] == trip_type for row in available_types)
        
        if type_exists:
            user_lang = session.get("userinfo", {}).get("lang", "en")
            lang_dict = lang.get(user_lang, {})
            
            stats["operators"] = {
                "km": get_stats_general(
                    pg=pg,
                    query_func=stats_sql.stats_operator_km,
                    user_id=user_id,
                    stat_name="operator",
                    trip_type=trip_type,
                    year=year,
                ),
                "trips": get_stats_general(
                    pg=pg,
                    query_func=stats_sql.stats_operator_trips,
                    user_id=user_id,
                    stat_name="operator",
                    trip_type=trip_type,
                    year=year,
                ),
            }
            
            stats["material"] = {
                "km": get_stats_general(
                    pg=pg,
                    query_func=stats_sql.stats_material_km,
                    user_id=user_id,
                    stat_name="material",
                    trip_type=trip_type,
                    year=year,
                ),
                "trips": get_stats_general(
                    pg=pg,
                    query_func=stats_sql.stats_material_trips,
                    user_id=user_id,
                    stat_name="material",
                    trip_type=trip_type,
                    year=year,
                ),
            }
            
            stats["countries"] = {
                "km": get_stats_countries(
                    pg=pg,
                    query_func=stats_sql.stats_countries,
                    user_id=user_id,
                    km=True,
                    trip_type=trip_type,
                    year=year,
                ),
                "trips": get_stats_countries(
                    pg=pg,
                    query_func=stats_sql.stats_countries,
                    user_id=user_id,
                    km=False,
                    trip_type=trip_type,
                    year=year,
                ),
            }
            
            stats["years"] = {
                "km": get_stats_years(
                    pg=pg,
                    query_func=stats_sql.stats_year_km,
                    user_id=user_id,
                    lang=lang_dict,
                    trip_type=trip_type,
                    year=year,
                ),
                "trips": get_stats_years(
                    pg=pg,
                    query_func=stats_sql.stats_year_trips,
                    user_id=user_id,
                    lang=lang_dict,
                    trip_type=trip_type,
                    year=year,
                ),
            }
            
            stats["routes"] = {
                "km": get_stats_general(
                    pg=pg,
                    query_func=stats_sql.stats_routes_km,
                    user_id=user_id,
                    stat_name="route",
                    trip_type=trip_type,
                    year=year,
                ),
                "trips": get_stats_general(
                    pg=pg,
                    query_func=stats_sql.stats_routes_trips,
                    user_id=user_id,
                    stat_name="route",
                    trip_type=trip_type,
                    year=year,
                ),
            }
            
            stats["stations"] = {
                "km": get_stats_general(
                    pg=pg,
                    query_func=stats_sql.stats_stations_km,
                    user_id=user_id,
                    stat_name="station",
                    trip_type=trip_type,
                    year=year,
                ),
                "trips": get_stats_general(
                    pg=pg,
                    query_func=stats_sql.stats_stations_trips,
                    user_id=user_id,
                    stat_name="station",
                    trip_type=trip_type,
                    year=year,
                ),
            }

    return stats


def get_distinct_stat_years(username, trip_type):
    """Get list of years with statistics available"""
    user_id = get_user_id(username)
    with pg_session() as pg:
        result = pg.execute(
            stats_sql.distinct_stat_years(),
            {"user_id": user_id, "tripType": trip_type}
        ).fetchall()
        return [row[0] for row in result]
