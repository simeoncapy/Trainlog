import csv
import io
import json
import logging
import logging.config
import sqlite3
import os
from pathlib import Path


from src.pg import get_or_create_pg_session, pg_session
from src.trips import Trip, compare_trip, parse_date
from src.utils import get_user_id, mainConn, pathConn, managed_cursor
from src.carbon import calculate_carbon_footprint_for_trip
from sqlalchemy import text

logging.config.fileConfig("logging.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def sync_db_from_sqlite():
    """
    Sync the PostgreSQL database with the SQLite database.
    Syncs trips, paths, and calculates carbon footprints.
   
    Uses IMMEDIATE locks on SQLite to completely prevent writes during sync.
    Uses SHARE mode locks on PostgreSQL to allow reads but prevent writes.
    Creates a lock file to signal the app that sync is in progress.
    """
    logger.info("Syncing SQLite database with PostgreSQL...")
    
    # Define lock file path
    lock_file = Path("db_sync.lock")  # Or use a path relevant to your app
    
    try:
        # Create lock file to signal sync in progress
        logger.info("Creating lock file...")
        lock_file.write_text(f"locked_at={os.getpid()}:{__import__('time').time()}")
        logger.info(f"Lock file created at: {lock_file}")
        
        # Lock SQLite databases with IMMEDIATE lock - blocks ALL writes
        logger.info("Acquiring IMMEDIATE locks on SQLite databases...")
        mainConn.isolation_level = None  # autocommit off
        mainConn.execute("BEGIN IMMEDIATE")
        pathConn.isolation_level = None
        pathConn.execute("BEGIN IMMEDIATE")
        logger.info("SQLite databases locked - reads allowed for this connection, ALL writes blocked")
       
        try:
            with pg_session() as pg:
                # Acquire SHARE locks on PG tables - allows reads, blocks writes
                logger.info("Acquiring PostgreSQL table locks...")
                pg.execute(text("LOCK TABLE trips IN SHARE MODE"))
                pg.execute(text("LOCK TABLE paths IN SHARE MODE"))
                logger.info("PostgreSQL tables locked - reads allowed, writes blocked")
               
                sync_trips_from_sqlite(pg)
                sync_paths_from_sqlite(pg)
                backfill_carbon_for_all_trips(pg)
               
                # Locks are automatically released when transaction commits
                logger.info("Sync complete - releasing locks")
        finally:
            # Release SQLite locks
            mainConn.rollback()
            pathConn.rollback()
            logger.info("SQLite locks released")
    finally:
        # Always remove lock file, even if sync fails
        if lock_file.exists():
            lock_file.unlink()
            logger.info("Lock file removed")

def trip_to_csv(trip: Trip):
    items = [
        trip.trip_id,
        trip.user_id,
        trip.origin_station,
        trip.destination_station,
        trip.start_datetime,
        trip.end_datetime,
        trip.is_project,
        trip.utc_start_datetime,
        trip.utc_end_datetime,
        trip.estimated_trip_duration,
        trip.manual_trip_duration,
        trip.trip_length,
        trip.operator,
        trip.countries,
        trip.line_name,
        trip.created,
        trip.last_modified,
        trip.type,
        trip.material_type,
        trip.seat,
        trip.reg,
        trip.waypoints,
        trip.notes,
        trip.price,
        trip.currency,
        trip.ticket_id,
        trip.purchasing_date,
    ]
    return items


def sync_trips_from_sqlite(pg_session=None):
    logger.info("Syncing trips from SQLite to PostgreSQL...")

    # fetch all trips from sqlite
    with managed_cursor(mainConn) as cursor:
        cursor.execute("SELECT count(*) FROM trip")
        num_trips = cursor.fetchone()[0]
        logger.info(f"Syncing {num_trips} trips from SQLite to PostgreSQL")

        cursor.execute("SELECT * FROM trip ORDER BY uid")
        sqlite_trips = cursor.fetchall()

    csv_buf = io.StringIO()
    csv_writer = csv.writer(csv_buf, delimiter="\t", quoting=csv.QUOTE_MINIMAL)

    for i, row in enumerate(sqlite_trips):
        if i % 20000 == 0:
            logger.info(f"Converting trip {i}/{num_trips}")

        start_datetime = (
            row["start_datetime"] if row["start_datetime"] not in [-1, 1] else None
        )
        parsed_start_datetime = parse_date(start_datetime) if start_datetime else None
        end_datetime = (
            row["end_datetime"] if row["end_datetime"] not in [-1, 1] else None
        )
        parsed_end_datetime = parse_date(end_datetime) if end_datetime else None
        parsed_utc_start_datetime = (
            parse_date(row["utc_start_datetime"]) if row["utc_start_datetime"] else None
        )
        parsed_utc_end_datetime = (
            parse_date(row["utc_end_datetime"]) if row["utc_end_datetime"] else None
        )
        trip = Trip(
            trip_id=row["uid"],
            username=row["username"],
            user_id=get_user_id(row["username"]),
            origin_station=row["origin_station"],
            destination_station=row["destination_station"],
            start_datetime=parsed_start_datetime,
            end_datetime=parsed_end_datetime,
            trip_length=row["trip_length"],
            estimated_trip_duration=row["estimated_trip_duration"],
            operator=row["operator"],
            countries=row["countries"],
            manual_trip_duration=row["manual_trip_duration"],
            utc_start_datetime=parsed_utc_start_datetime,
            utc_end_datetime=parsed_utc_end_datetime,
            created=row["created"],
            last_modified=row["last_modified"],
            line_name=row["line_name"],
            type=row["type"],
            material_type=row["material_type"],
            seat=row["seat"],
            reg=row["reg"],
            waypoints=row["waypoints"],
            notes=row["notes"],
            price=row["price"] if row["price"] != "" else None,
            currency=row["currency"],
            purchasing_date=row["purchasing_date"]
            if row["purchasing_date"] != ""
            else None,
            ticket_id=row["ticket_id"] if row["ticket_id"] != "" else None,
            is_project=row["start_datetime"] == 1 or row["end_datetime"] == 1,
            path=None,  # not needed when inserting trips
        )
        csv_writer.writerow(trip_to_csv(trip))

    csv_buf.seek(0)

    with get_or_create_pg_session(pg_session) as pg:
        # remove existing trips from pg
        logger.info("Deleting existing trips in pg...")
        query = "DELETE FROM trips;"
        pg.execute(query)

        query = """
            COPY trips (
                trip_id,
                user_id,
                origin_station,
                destination_station,
                start_datetime,
                end_datetime,
                is_project,
                utc_start_datetime,
                utc_end_datetime,
                estimated_trip_duration,
                manual_trip_duration,
                trip_length,
                operator,
                countries,
                line_name,
                created,
                last_modified,
                trip_type,
                material_type,
                seat,
                reg,
                waypoints,
                notes,
                price,
                currency,
                ticket_id,
                purchase_date
            ) FROM STDIN WITH (
                FORMAT csv,
                DELIMITER E'\t',
                QUOTE '"'
            )
        """

        logger.info("Bulk inserting trips in pg...")
        cursor = pg.connection().connection.cursor()
        cursor.copy_expert(query, csv_buf)
    logger.info("Finished migrating trips from sqlite to pg!")


def sync_paths_from_sqlite(pg_session=None):
    """
    Sync paths from paths.db SQLite to PostgreSQL paths table.
    """
    logger.info("Syncing paths from SQLite to PostgreSQL...")
    
    # Get all paths from SQLite
    cursor = pathConn.cursor()
    cursor.execute("SELECT count(*) FROM paths")
    num_paths = cursor.fetchone()[0]
    logger.info(f"Syncing {num_paths} paths from SQLite to PostgreSQL")
    
    cursor.execute("SELECT trip_id, path FROM paths ORDER BY trip_id")
    sqlite_paths = cursor.fetchall()
    
    with get_or_create_pg_session(pg_session) as pg:
        # Prepare batch insert
        batch_size = 1000
        batch = []
        
        for i, row in enumerate(sqlite_paths):
            if i % 10000 == 0:
                logger.info(f"Converting path {i}/{num_paths}")
            
            trip_id = row['trip_id']
            path_str = row['path']
            
            # Convert [[X,Y],[X,Y]] string to WKT LINESTRING
            try:
                # Parse the string representation of the coordinates
                coordinates = json.loads(path_str)
                
                if coordinates:
                    if len(coordinates) >= 2:
                        # Create WKT LINESTRING format
                        wkt_coords = ', '.join([f"{lon} {lat}" for lon, lat in coordinates])
                        wkt = f"LINESTRING({wkt_coords})"
                    elif len(coordinates) == 1:
                        lon, lat = coordinates[0]
                        wkt = f"POINT({lon} {lat})"
                    else:
                        logger.warning(f"Trip {trip_id} has empty coordinate list: {path_str}")
                        continue

                    batch.append((trip_id, wkt))

                    # Insert in batches
                    if len(batch) >= batch_size:
                        insert_paths_batch(pg, batch)
                        batch = []
                else:
                    logger.warning(f"Trip {trip_id} has invalid path: {path_str}")

                    
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"Failed to parse path for trip {trip_id}: {e}")
                continue
        
        # Insert remaining paths
        if batch:
            insert_paths_batch(pg, batch)
            
        pg.commit()
    logger.info("Finished migrating paths from sqlite to pg!")


def insert_paths_batch(pg, batch):
    """
    Insert a batch of paths into PostgreSQL using SQLAlchemy and PostGIS.
    """
    if not batch:
        return

    # Build the parameter list as dicts for SQLAlchemy
    param_dicts = [
        {"trip_id": trip_id, "wkt": wkt}
        for trip_id, wkt in batch
    ]

    query = text("""
        INSERT INTO paths (trip_id, path)
        VALUES (:trip_id, ST_GeomFromText(:wkt, 4326))
        ON CONFLICT (trip_id) DO UPDATE
        SET path = EXCLUDED.path
    """)

    pg.execute(query, param_dicts)

def backfill_carbon_for_all_trips(pg_session=None, commit_every=10000):
    """
    Calculate and update carbon footprint for all trips using PostgreSQL only.
    Only pass path data to the carbon calculator for air trips.
    """
    logger.info("Calculating carbon footprint for all trips (PostgreSQL only)...")

    with get_or_create_pg_session(pg_session) as pg:
        trips = pg.execute(text("""
            SELECT
                trip_id,
                trip_type,
                trip_length,
                countries,
                start_datetime::text AS start_datetime,
                end_datetime::text   AS end_datetime,
                estimated_trip_duration,
                manual_trip_duration
            FROM trips
            ORDER BY trip_id
        """)).fetchall()

        total = len(trips)
        logger.info(f"Found {total} trips to calculate carbon for")

        for idx, row in enumerate(trips, 1):
            if idx % commit_every == 0:
                logger.info(f"Progress: {idx}/{total} trips processed")

            try:
                trip = dict(row._mapping)
            except AttributeError:
                trip = dict(row)

            trip_id = trip["trip_id"]
            trip_type = trip["trip_type"]

            # Only fetch/prepare path for air trips
            path_data = None
            if trip_type in ("air", "helicopter"):
                path_row = pg.execute(
                    text("SELECT ST_AsGeoJSON(path) AS path_json FROM paths WHERE trip_id = :trip_id"),
                    {"trip_id": trip_id}
                ).fetchone()
                if path_row:
                    try:
                        path_json = (dict(path_row._mapping) if hasattr(path_row, "_mapping") else dict(path_row)).get("path_json")
                        if path_json:
                            gj = json.loads(path_json)
                            if gj.get("type") == "LineString":
                                # GeoJSON coords are [lon, lat]
                                path_data = [{"lat": lat, "lng": lon} for lon, lat in gj.get("coordinates", [])]
                            elif gj.get("type") == "Point":
                                coords = gj.get("coordinates", [])
                                if len(coords) >= 2:
                                    path_data = [{"lat": coords[1], "lng": coords[0]}]
                    except Exception as e:
                        logger.warning(f"Trip {trip_id}: could not parse path GeoJSON: {e}")

            trip_data = {
                "trip_id": trip_id,
                "type": trip.get("trip_type"),
                "trip_length": trip.get("trip_length"),
                "countries": trip.get("countries"),
                "start_datetime": trip.get("start_datetime"),   # strings now
                "end_datetime": trip.get("end_datetime"),       # strings now
                "estimated_trip_duration": trip.get("estimated_trip_duration"),
                "manual_trip_duration": trip.get("manual_trip_duration"),
            }

            carbon = calculate_carbon_footprint_for_trip(trip_data, path_data)
           

            pg.execute(
                text("UPDATE trips SET carbon = :carbon WHERE trip_id = :trip_id"),
                {"carbon": carbon, "trip_id": trip_id}
            )

            if idx % commit_every == 0:
                pg.commit()

        pg.commit()
        logger.info(f"Carbon backfill complete: {total} trips processed")

def compare_all_trips():
    # Fetch trip IDs from SQLite
    with managed_cursor(mainConn) as cursor:
        cursor.execute("SELECT uid FROM trip ORDER BY uid")
        sqlite_trips = {row[0] for row in cursor.fetchall()}

    # Fetch trip IDs from PostgreSQL
    with pg_session() as pg:
        pg_trips = {row[0] for row in pg.execute("SELECT trip_id FROM trips ORDER BY trip_id").fetchall()}

    # Compare the counts
    if len(sqlite_trips) != len(pg_trips):
        only_in_sqlite = sqlite_trips - pg_trips
        only_in_pg = pg_trips - sqlite_trips

        msg = (
            f"Mismatch in trip counts! "
            f"SQLite has {len(sqlite_trips)} trips, PG has {len(pg_trips)} trips.\n"
            f"Trips only in SQLite: {sorted(only_in_sqlite)}\n"
            f"Trips only in PG: {sorted(only_in_pg)}"
        )
        logger.error(msg)
        raise Exception(msg)

    # If counts match, do full comparison
    try:
        for i, trip_id in enumerate(sorted(sqlite_trips)):
            if i % 20000 == 0:
                logger.info(f"Checking consistency of trip {i}/{len(sqlite_trips)}")
            compare_trip(trip_id)
    except Exception:
        logger.error(f"Found exception while processing trip {trip_id}")
        raise


def compare_all_paths():
    """
    Compare paths between SQLite and PostgreSQL to ensure consistency.
    """
    logger.info("Comparing paths between SQLite and PostgreSQL...")
    
    
    try:
        # Get path trip IDs from SQLite
        cursor = pathConn.cursor()
        cursor.execute("SELECT trip_id FROM paths ORDER BY trip_id")
        sqlite_path_ids = {row[0] for row in cursor.fetchall()}
        
        # Get path trip IDs from PostgreSQL
        with pg_session() as pg:
            pg_path_ids = {row[0] for row in pg.execute("SELECT trip_id FROM paths ORDER BY trip_id").fetchall()}
        
        # Compare counts
        if len(sqlite_path_ids) != len(pg_path_ids):
            only_in_sqlite = sqlite_path_ids - pg_path_ids
            only_in_pg = pg_path_ids - sqlite_path_ids
            
            msg = (
                f"Mismatch in path counts! "
                f"SQLite has {len(sqlite_path_ids)} paths, PG has {len(pg_path_ids)} paths.\n"
                f"Paths only in SQLite: {sorted(only_in_sqlite)}\n"
                f"Paths only in PG: {sorted(only_in_pg)}"
            )
            logger.error(msg)
            raise Exception(msg)
        
        logger.info(f"Path counts match: {len(sqlite_path_ids)} paths in both databases")
        
    finally:
        pathConn.close()