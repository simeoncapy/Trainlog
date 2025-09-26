def convert_graphhopper_to_osrm(gh_response):
    """Convert GraphHopper response to OSRM format for compatibility"""
    # Rest of your existing converter code...
    if not gh_response.get('paths'):
        return {"code": "NoRoute", "message": "No route found"}
   
    path = gh_response['paths'][0]
   
    # Decode polyline to get actual coordinates
    encoded_points = path.get('points', '')
    if encoded_points:
        # GraphHopper uses precision 5 by default
        coordinates = decode_polyline(encoded_points, precision=5)
       
        # Create waypoints from first and last coordinates
        waypoints = []
        if len(coordinates) >= 2:
            # Start point
            waypoints.append({
                "name": "",
                "location": [coordinates[0][1], coordinates[0][0]],  # [lng, lat]
                "distance": 0,
                "hint": "",
                "waypoint_index": 0
            })
            # End point
            waypoints.append({
                "name": "",
                "location": [coordinates[-1][1], coordinates[-1][0]],  # [lng, lat]
                "distance": 0,
                "hint": "",
                "waypoint_index": 1
            })
    else:
        # Fallback to bbox if no encoded points
        bbox = path.get('bbox', [])
        waypoints = []
        if len(bbox) >= 4:
            waypoints.append({
                "name": "",
                "location": [bbox[0], bbox[1]],  # [lng, lat]
                "distance": 0,
                "hint": "",
                "waypoint_index": 0
            })
            waypoints.append({
                "name": "",
                "location": [bbox[2], bbox[3]],  # [lng, lat]
                "distance": 0,
                "hint": "",
                "waypoint_index": 1
            })
   
    # Create legs structure (required by Leaflet Routing Machine)
    legs = [{
        "distance": path.get('distance', 0),
        "duration": path.get('time', 0) / 1000,  # Convert ms to seconds
        "summary": "",
        "steps": [],  # Empty steps array
        "weight": path.get('weight', 0),
        "weight_name": "routability",
        "annotation": {
            "distance": [path.get('distance', 0)],
            "duration": [path.get('time', 0) / 1000]
        }
    }]
   
    # Build OSRM-compatible response
    osrm_response = {
        "code": "Ok",
        "routes": [{
            "geometry": encoded_points,  # Keep the encoded polyline
            "distance": path.get('distance', 0),
            "duration": path.get('time', 0) / 1000,  # Convert ms to seconds
            "weight": path.get('weight', 0),
            "weight_name": "routability",
            "legs": legs,
            "details": path.get("details", 0)
        }],
        "waypoints": waypoints
    }
   
    return osrm_response

def decode_polyline(encoded, precision=5):
    """Decode a polyline string into a list of lat/lng tuples."""
    coordinates = []
    index = 0
    lat = 0
    lng = 0
    factor = 10 ** precision
   
    while index < len(encoded):
        # Latitude
        shift = 0
        result = 0
        while True:
            byte = ord(encoded[index]) - 63
            index += 1
            result |= (byte & 0x1f) << shift
            shift += 5
            if byte < 0x20:
                break
        lat += (~result >> 1) if (result & 1) else (result >> 1)
       
        # Longitude
        shift = 0
        result = 0
        while True:
            byte = ord(encoded[index]) - 63
            index += 1
            result |= (byte & 0x1f) << shift
            shift += 5
            if byte < 0x20:
                break
        lng += (~result >> 1) if (result & 1) else (result >> 1)
       
        coordinates.append((lat / factor, lng / factor))
   
    return coordinates