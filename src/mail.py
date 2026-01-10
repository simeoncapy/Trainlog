from imapclient import IMAPClient
import threading
import email as email_lib
from email.header import decode_header
from email.utils import parseaddr
import time
import logging
import json
import os
import requests
from datetime import datetime

from py.utils import load_config, getCountryFromCoordinates, get_flag_emoji, getDistance
from src.trips import Trip, create_trip
from src.users import User
from src.consts import Env

logger = logging.getLogger(__name__)
_app = None

def get_email_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(errors="ignore")
            elif part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(errors="ignore")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(errors="ignore")
    return ""

def get_user_from_sender(sender_raw):
    _, email_address = parseaddr(sender_raw)
    email_address = email_address.lower()
    
    user = User.query.filter_by(email=email_address).first()
    if not user:
        logger.info(f"No user found for email: {email_address}")
        return None
    
    if not user.premium:
        logger.info(f"User {user.username} is not premium, ignoring email")
        return None
    env = os.environ["ENVIRONMENT"]
    if env != Env.PROD.value:
        logger.info(f"Only process emails in Prod, env = {env}, Env.PROD = {Env.PROD.value}")
        return None
    
    return user

def get_airport_by_iata(iata):
    from src.utils import mainConn, managed_cursor
    with managed_cursor(mainConn) as cursor:
        result = cursor.execute(
            "SELECT name, latitude, longitude, iso_country FROM airports WHERE iata = ?",
            (iata.upper(),)
        ).fetchone()
        if result:
            return dict(result)
    return None

def geocode_station(query):
    try:
        response = requests.get(
            "https://photon.komoot.io/api",
            params={"q": query, "limit": 1, "lang": "en"},
            timeout=5
        )
        data = response.json()
        if data.get("features"):
            feat = data["features"][0]
            props = feat["properties"]
            lon, lat = feat["geometry"]["coordinates"]
            country_code = props.get("countrycode", "")
            if not country_code:
                country = getCountryFromCoordinates(lat, lon)
                country_code = country.get("countryCode", "")
            return {
                "name": props.get("name", query),
                "lat": lat,
                "lng": lon,
                "country_code": country_code
            }
    except Exception as e:
        logger.error(f"Geocoding error: {e}")
    return None

def parse_ticket_with_ai(subject, body, user_lang="en"):
    config = load_config()
    api_key = config.get("mistral", {}).get("api_key")
    if not api_key:
        logger.error("No Mistral API key found")
        return None
    
    lang_names = {
        "en": "English", "fr": "French", "de": "German", "es": "Spanish",
        "it": "Italian", "pt": "Portuguese", "nl": "Dutch", "pl": "Polish",
        "cs": "Czech", "ja": "Japanese", "zh": "Chinese", "ko": "Korean"
    }
    lang_name = lang_names.get(user_lang, "English")
    
    prompt = f"""Extract all trips from this ticket confirmation email.
A trip is ONE segment (e.g., a flight with one connection = 2 trips).

Return ONLY valid JSON array, no markdown:
[{{
  "type": "train|air|bus|ferry|tram|metro",
  "origin": "City or Station name",
  "origin_iata": "ABC or null if not a flight",
  "destination": "City or Station name", 
  "destination_iata": "XYZ or null if not a flight",
  "date": "YYYY-MM-DD",
  "time_departure": "HH:MM or null",
  "time_arrival": "HH:MM or null",
  "operator": "Company name",
  "line_name": "Flight number or train number or null",
  "price": number or null,
  "currency": "EUR|USD|etc or null",
  "aircraft_icao": "ICAO code like B738, A320, E295 or null if not a flight or unknown",
  "seat": "Seat number like 12A or null",
  "booking_reference": "PNR/booking code or null",
  "ticket_number": "Ticket number or null",
  "cabin_class": "Economy/Business/First or null",
  "notes": "Any other relevant info in {lang_name}, or null"
}}]

For aircraft types, use ICAO codes: Boeing 737-800=B738, Airbus A320=A320, Embraer 195=E195, etc.

If not a valid ticket, return []

Subject: {subject}
Body: {body}"""

    try:
        response = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "mistral-small-latest",
                "messages": [{"role": "user", "content": prompt}]
            }
        )
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        return json.loads(content)
    except Exception as e:
        logger.error(f"AI parsing error: {e}")
        return None

def build_notes(parsed_trip, user_lang="en"):
    parts = ["From Trainlog email parser"]
    
    if parsed_trip.get("booking_reference"):
        parts.append(f"PNR: {parsed_trip['booking_reference']}")
    
    if parsed_trip.get("ticket_number"):
        parts.append(f"Ticket: {parsed_trip['ticket_number']}")
    
    if parsed_trip.get("cabin_class"):
        parts.append(f"Class: {parsed_trip['cabin_class']}")
    
    if parsed_trip.get("notes"):
        parts.append(parsed_trip["notes"])
    
    return " | ".join(parts)

def create_trip_from_parsed(user, parsed_trip):
    from timezonefinder import TimezoneFinder
    import pytz
    
    trip_type = parsed_trip.get("type", "train")
    now = datetime.now()
    
    if trip_type == "air":
        origin_iata = parsed_trip.get("origin_iata")
        dest_iata = parsed_trip.get("destination_iata")
        
        origin_airport = get_airport_by_iata(origin_iata) if origin_iata else None
        dest_airport = get_airport_by_iata(dest_iata) if dest_iata else None
        
        if not origin_airport or not dest_airport:
            logger.error(f"Could not find airports: {origin_iata}, {dest_iata}")
            return None
        
        origin_flag = get_flag_emoji(origin_airport["iso_country"])
        dest_flag = get_flag_emoji(dest_airport["iso_country"])
        
        origin_station = f"{origin_flag} {origin_airport['name']} ({origin_iata.upper()})"
        dest_station = f"{dest_flag} {dest_airport['name']} ({dest_iata.upper()})"
        
        path = [
            {"lat": origin_airport["latitude"], "lng": origin_airport["longitude"]},
            {"lat": dest_airport["latitude"], "lng": dest_airport["longitude"]}
        ]
        
        trip_length = getDistance(path[0], path[-1])
        origin_country = getCountryFromCoordinates(path[0]["lat"], path[0]["lng"])
        dest_country = getCountryFromCoordinates(path[-1]["lat"], path[-1]["lng"])
        countries = json.dumps({
            origin_country["countryCode"]: trip_length / 2,
            dest_country["countryCode"]: trip_length / 2
        })
        
        material_type = parsed_trip.get("aircraft_icao")
    else:
        origin_geo = geocode_station(parsed_trip["origin"])
        dest_geo = geocode_station(parsed_trip["destination"])
        
        if not origin_geo or not dest_geo:
            logger.error(f"Could not geocode: {parsed_trip['origin']}, {parsed_trip['destination']}")
            return None
        
        origin_flag = get_flag_emoji(origin_geo["country_code"])
        dest_flag = get_flag_emoji(dest_geo["country_code"])
        
        origin_station = f"{origin_flag} {origin_geo['name']}"
        dest_station = f"{dest_flag} {dest_geo['name']}"
        
        path = [
            {"lat": origin_geo["lat"], "lng": origin_geo["lng"]},
            {"lat": dest_geo["lat"], "lng": dest_geo["lng"]}
        ]
        
        trip_length = getDistance(path[0], path[-1])
        countries = "{}"
        material_type = None
    
    trip_date = parsed_trip.get("date")
    dep_time = parsed_trip.get("time_departure")
    arr_time = parsed_trip.get("time_arrival")
    
    start_datetime = None
    end_datetime = None
    utc_start_datetime = None
    utc_end_datetime = None
    estimated_duration = None
    
    tf = TimezoneFinder()
    
    if trip_date:
        if dep_time:
            start_datetime = datetime.strptime(f"{trip_date} {dep_time}", "%Y-%m-%d %H:%M")
            origin_tz_name = tf.timezone_at(lat=path[0]["lat"], lng=path[0]["lng"])
            if origin_tz_name:
                origin_tz = pytz.timezone(origin_tz_name)
                local_start = origin_tz.localize(start_datetime)
                utc_start_datetime = local_start.astimezone(pytz.UTC).replace(tzinfo=None)
        else:
            start_datetime = datetime.strptime(f"{trip_date} 00:00:01", "%Y-%m-%d %H:%M:%S")
        
        if arr_time:
            end_datetime = datetime.strptime(f"{trip_date} {arr_time}", "%Y-%m-%d %H:%M")
            dest_tz_name = tf.timezone_at(lat=path[-1]["lat"], lng=path[-1]["lng"])
            if dest_tz_name:
                dest_tz = pytz.timezone(dest_tz_name)
                local_end = dest_tz.localize(end_datetime)
                utc_end_datetime = local_end.astimezone(pytz.UTC).replace(tzinfo=None)
        else:
            end_datetime = start_datetime
        
        if dep_time and arr_time and utc_start_datetime and utc_end_datetime:
            estimated_duration = int((utc_end_datetime - utc_start_datetime).total_seconds())
    
    is_project = False
    
    notes = build_notes(parsed_trip, user.lang)
    
    trip = Trip(
        username=user.username,
        user_id=user.uid,
        origin_station=origin_station,
        destination_station=dest_station,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        utc_start_datetime=utc_start_datetime,
        utc_end_datetime=utc_end_datetime,
        trip_length=trip_length,
        estimated_trip_duration=estimated_duration,
        manual_trip_duration=None,
        operator=parsed_trip.get("operator"),
        line_name=parsed_trip.get("line_name"),
        created=now,
        last_modified=now,
        type=trip_type,
        price=parsed_trip.get("price"),
        currency=parsed_trip.get("currency"),
        purchasing_date=now,
        ticket_id=None,
        is_project=is_project,
        path=path,
        countries=countries,
        seat=parsed_trip.get("seat"),
        material_type=material_type,
        reg=None,
        waypoints=None,
        notes=notes,
    )
    
    create_trip(trip)
    logger.info(f"Created trip for {user.username}")
    return trip


def process_incoming_email(raw):
    msg = email_lib.message_from_bytes(raw)
    sender = msg["From"]
    
    with _app.app_context():
        user = get_user_from_sender(sender)
        if not user:
            return
        
        subject, enc = decode_header(msg["Subject"])[0]
        if isinstance(subject, bytes):
            subject = subject.decode(enc or "utf-8")
        body = get_email_body(msg)
        
        logger.info(f"Processing email from {user.username}")
        
        trips = parse_ticket_with_ai(subject, body, user.lang)
        
        if trips:
            logger.info(f"Found {len(trips)} trip(s)")
            for parsed in trips:
                create_trip_from_parsed(user, parsed)
        else:
            logger.info("No trips found in email")

def email_listener():
    config = load_config()
    cfg = config.get("email_receiver")
    if not cfg:
        logger.warning("No email_receiver config found, skipping email listener")
        return
    
    while True:
        try:
            client = IMAPClient(cfg["imap"], ssl=True)
            client.login(cfg["user"], cfg["password"])
            client.select_folder("INBOX")
            logger.info("Email listener connected")
            
            while True:
                client.idle()
                responses = client.idle_check(timeout=300)
                client.idle_done()
                
                if responses:
                    messages = client.search("UNSEEN")
                    for msg_id in messages:
                        raw = client.fetch([msg_id], ["RFC822"])[msg_id][b"RFC822"]
                        process_incoming_email(raw)
                        
        except Exception as e:
            logger.error(f"Email listener error: {e}")
            time.sleep(10)

def start_email_listener(app):
    global _app
    _app = app
    thread = threading.Thread(target=email_listener, daemon=True)
    thread.start()