#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from typing import List
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests
import pycountry
import osm2geojson


OVERPASS_URL_DEFAULT = "https://overpass-api.de/api/interpreter"


def pg_connect(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def clean_tables(conn):
    """Delete all data from completion tables"""
    print("🧹 Cleaning existing data...")
    with conn.cursor() as cur:
        cur.execute("DELETE FROM coverage_unit_area_by_area")
        cur.execute("DELETE FROM user_traveled_unit")
        cur.execute("DELETE FROM coverage_unit")
        cur.execute("DELETE FROM admin_area")
    conn.commit()
    print("✓ Tables cleaned")


def upsert_country(conn, iso_code: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT admin_area_id FROM admin_area WHERE iso_code = %s AND level = 1", (iso_code,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(
            """
            INSERT INTO admin_area (iso_code, level, parent_admin_area_id, name, geom)
            VALUES (%s, 1, NULL, %s, NULL)
            RETURNING admin_area_id
            """,
            (iso_code, iso_code),
        )
        return cur.fetchone()[0]


def ensure_region_row(conn, iso_region: str) -> int:
    parts = iso_region.split("-", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid region code: {iso_region}")
    parent_iso = parts[0]
    
    with conn.cursor() as cur:
        cur.execute("SELECT admin_area_id FROM admin_area WHERE iso_code = %s AND level = 1", (parent_iso,))
        parent_row = cur.fetchone()
        if not parent_row:
            cur.execute(
                """
                INSERT INTO admin_area (iso_code, level, parent_admin_area_id, name, geom)
                VALUES (%s, 1, NULL, %s, NULL)
                RETURNING admin_area_id
                """,
                (parent_iso, parent_iso),
            )
            parent_row = cur.fetchone()
        parent_id = parent_row[0]

        cur.execute("SELECT admin_area_id FROM admin_area WHERE iso_code = %s AND level = 2", (iso_region,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(
            """
            INSERT INTO admin_area (iso_code, level, parent_admin_area_id, name, geom)
            VALUES (%s, 2, %s, %s, NULL)
            RETURNING admin_area_id
            """,
            (iso_region, parent_id, iso_region),
        )
        return cur.fetchone()[0]


def insert_country_units_bulk(conn, admin_area_id: int, iso_code: str, file_path: str) -> int:
    with open(file_path, "r", encoding="utf-8") as f:
        gj = json.load(f)

    features = gj.get("features", [])
    if not features:
        return 0

    with conn.cursor() as cur:
        inserted = 0
        for idx, feat in enumerate(features):
            geom = feat.get("geometry")
            if not geom:
                continue
            props = feat.get("properties") or {}
            src_id = props.get("id") or f"{iso_code}:{idx}"
            geom_json = json.dumps(geom, separators=(",", ":"))
            
            cur.execute(
                """
                INSERT INTO coverage_unit (admin_area_id, source_feature_id, geom, properties_json)
                VALUES (%s, %s, ST_Force2D(ST_SetSRID(ST_GeomFromGeoJSON(%s),4326)), %s)
                ON CONFLICT (admin_area_id, source_feature_id) DO NOTHING
                """,
                (admin_area_id, src_id, geom_json, json.dumps(props)),
            )
            if cur.rowcount > 0:
                inserted += 1
    
    return inserted


def union_area_units_into_admin_area(conn, admin_area_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE admin_area a
               SET geom = sub.geom
              FROM (
                    SELECT ST_Multi(ST_Union(cu.geom)) AS geom
                      FROM coverage_unit cu
                     WHERE cu.admin_area_id = %s
                   ) sub
             WHERE a.admin_area_id = %s
            """,
            (admin_area_id, admin_area_id),
        )


def overpass_region_geojson(iso_region: str, overpass_url: str, retries: int = 3, backoff: float = 2.0) -> dict:
    query = f"""
    [out:json];
    relation["ISO3166-2"="{iso_region}"];
    (._; >;);
    out body;
    """
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(overpass_url, params={"data": query}, timeout=120)
            r.raise_for_status()
            osm_json = r.json()
            geojson = osm2geojson.json2geojson(osm_json, filter_used_refs=True, log_level="ERROR")
            return geojson
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Overpass fetch failed for {iso_region}: {last_exc}")


def upsert_region_boundary(conn, iso_region: str, region_gj: dict) -> int:
    region_id = ensure_region_row(conn, iso_region)
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH feats AS (
              SELECT ST_SetSRID(ST_Force2D(ST_GeomFromGeoJSON(f->>'geometry')),4326) AS g
              FROM json_array_elements(%s::json->'features') f
              WHERE (f->>'geometry') IS NOT NULL
            )
            UPDATE admin_area a
               SET geom = (SELECT ST_Multi(ST_Union(g)) FROM feats)
             WHERE a.admin_area_id = %s
            """,
            (json.dumps(region_gj, separators=(",", ":")), region_id),
        )
    return region_id


def populate_region_cache(conn, region_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT populate_region_unit_areas(%s)", (region_id,))


def list_subdivision_codes(iso_country: str) -> List[str]:
    country = (
        pycountry.countries.get(alpha_2=iso_country)
        or pycountry.countries.get(alpha_3=iso_country)
        or pycountry.countries.get(numeric=iso_country)
    )
    if not country:
        return []
    out = []
    for s in list(pycountry.subdivisions.get(country_code=country.alpha_2)):
        if s.parent_code is None:
            out.append(s.code)
    return out


def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def main():
    ap = argparse.ArgumentParser(description="Import country GeoJSON coverage data to PostGIS")
    ap.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    ap.add_argument("--dir", required=True, help="Directory with *.geojson files")
    ap.add_argument("--commit-every", type=int, default=5, help="Commit after N countries")
    ap.add_argument("--countries", default=None, help="Comma-separated ISO codes (e.g. FR,DE)")
    ap.add_argument("--fetch-regions", action="store_true", help="Fetch region boundaries from Overpass")
    ap.add_argument("--overpass-url", default=OVERPASS_URL_DEFAULT, help="Overpass API endpoint")
    ap.add_argument("--region-cache", action="store_true", help="Populate region cache")
    ap.add_argument("--clean", action="store_true", default=True, help="Clean tables before import")
    args = ap.parse_args()

    conn = pg_connect(args.dsn)

    if args.clean:
        clean_tables(conn)

    processed = 0
    total_units = 0
    region_count = 0
    skipped = 0
    errors = 0
    start_time = time.time()

    try:
        if args.countries:
            country_codes = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
            file_list = [(c, os.path.join(args.dir, f"{c.lower()}.geojson")) for c in country_codes]
        else:
            file_list = []
            for fname in sorted(os.listdir(args.dir)):
                if not fname.lower().endswith(".geojson"):
                    continue
                iso = os.path.splitext(fname)[0].upper()
                if "-" in iso:
                    continue
                file_list.append((iso, os.path.join(args.dir, fname)))

        total_files = len(file_list)
        print(f"📦 Found {total_files} country files to import\n")

        for idx, (iso, fp) in enumerate(file_list, 1):
            if not os.path.isfile(fp):
                print(f"[{idx}/{total_files}] ⚠️  {iso}: File not found")
                skipped += 1
                continue

            try:
                country_id = upsert_country(conn, iso)
                inserted = insert_country_units_bulk(conn, country_id, iso, fp)
                union_area_units_into_admin_area(conn, country_id)
                
                processed += 1
                total_units += inserted
                
                elapsed = time.time() - start_time
                avg_time = elapsed / processed
                remaining = (total_files - processed) * avg_time
                pct = (processed / total_files) * 100
                
                print(f"[{idx}/{total_files}] ✓ {iso}: {inserted} units | {pct:.1f}% | ETA: {format_time(remaining)}")
                
                if processed % args.commit_every == 0:
                    conn.commit()

                if args.fetch_regions:
                    subdivisions = list_subdivision_codes(iso)
                    if subdivisions:
                        print(f"  └─ Fetching {len(subdivisions)} regions...")
                    for region_iso in subdivisions:
                        try:
                            gj = overpass_region_geojson(region_iso, args.overpass_url)
                            region_id = upsert_region_boundary(conn, region_iso, gj)
                            if args.region_cache:
                                populate_region_cache(conn, region_id)
                            region_count += 1
                            print(f"     ✓ {region_iso}")
                            time.sleep(1.0)
                        except Exception as e:
                            print(f"     ✗ {region_iso}: {e}", file=sys.stderr)

            except json.JSONDecodeError as e:
                print(f"[{idx}/{total_files}] ✗ {iso}: Invalid JSON - {e}")
                errors += 1
                conn.rollback()
            except Exception as e:
                print(f"[{idx}/{total_files}] ✗ {iso}: {e}")
                errors += 1
                conn.rollback()

        conn.commit()

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        conn.rollback()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}", file=sys.stderr)
        conn.rollback()
        raise
    finally:
        conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"✅ Import complete in {format_time(elapsed)}")
    print(f"   Countries processed: {processed}")
    print(f"   Coverage units: {total_units}")
    print(f"   Regions fetched: {region_count}")
    print(f"   Skipped: {skipped}")
    print(f"   Errors: {errors}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()