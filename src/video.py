"""
GPS Track Video Generator - Phase 1 Improvements
- Removed speedometer functionality
- Implemented constant speed through distance-based interpolation
- Added high-quality satellite and terrain tile providers
- Improved performance with better caching
"""

import os
import tempfile
import shutil
from typing import List, Dict, Tuple, Optional, Callable
import xml.etree.ElementTree as ET
from datetime import datetime
import math
import subprocess
from PIL import Image, ImageDraw, ImageFont
import requests
from io import BytesIO
import json
import numpy as np
from dataclasses import dataclass
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class GPSPoint:
    """Represents a single GPS point"""
    lat: float
    lng: float
    elevation: Optional[float] = None
    timestamp: Optional[datetime] = None
    distance_from_start: float = 0.0  # New: cumulative distance from start

@dataclass
class TrackBounds:
    """Bounding box for GPS track"""
    north: float
    south: float
    east: float
    west: float

@dataclass
class TrackLeg:
    """Represents one leg of a multi-leg trip"""
    name: str
    points: List[GPSPoint]
    color: str
    total_distance: float = 0.0
    duration: Optional[float] = None
    elevation_gain: float = 0.0

@dataclass
class VideoSettings:
    """Video generation settings"""
    width: int = 1920
    height: int = 1080
    fps: int = 30
    duration: int = 30  # seconds
    track_color: str = "#FF4444"
    marker_color: str = "#FF6B6B"
    marker_size: int = 12
    track_width: int = 4
    show_trail: bool = True
    show_data_overlay: bool = True
    background_color: str = "#87CEEB"
    map_style: str = "satellite"  # Changed default to satellite
    default_colors: List[str] = None
    constant_speed: bool = True  # New: ensure constant playback speed
    
    def __post_init__(self):
        if self.default_colors is None:
            self.default_colors = [
                "#FF4444",  # Red
                "#44FF44",  # Green  
                "#4444FF",  # Blue
                "#FFFF44",  # Yellow
                "#FF44FF",  # Magenta
                "#44FFFF",  # Cyan
                "#FF8844",  # Orange
                "#8844FF",  # Purple
                "#44FF88",  # Light Green
                "#FF4488"   # Pink
            ]

class GPXProcessor:
    """Processes GPX files and extracts track data"""
    
    @staticmethod
    def parse_multiple_gpx_files(gpx_files: List[Dict]) -> Dict:
        """Parse multiple GPX files for multi-leg trips with constant speed"""
        if not gpx_files:
            raise ValueError("No GPX files provided")
        
        default_colors = VideoSettings().default_colors
        track_legs = []
        all_points = []
        
        for i, gpx_info in enumerate(gpx_files):
            file_path = gpx_info['path']
            leg_name = gpx_info.get('name', f"Leg {i+1}")
            leg_color = gpx_info.get('color', default_colors[i % len(default_colors)])
            
            logger.info(f"Processing GPX file {i+1}/{len(gpx_files)}: {file_path}")
            
            single_track_data = GPXProcessor.parse_gpx_file(file_path)
            
            leg = TrackLeg(
                name=leg_name,
                points=single_track_data['points'],
                color=leg_color,
                total_distance=single_track_data['total_distance'],
                duration=single_track_data.get('duration'),
                elevation_gain=single_track_data.get('elevation_gain', 0.0)
            )
            
            track_legs.append(leg)
            all_points.extend(single_track_data['points'])
            
            logger.info(f"  Leg '{leg_name}': {len(leg.points)} points, {leg.total_distance:.2f}km")
        
        if not all_points:
            raise ValueError("No valid GPS points found in any GPX file")
        
        # Calculate combined bounds
        lats = [p.lat for p in all_points]
        lngs = [p.lng for p in all_points]
        combined_bounds = TrackBounds(
            north=max(lats),
            south=min(lats),
            east=max(lngs),
            west=min(lngs)
        )
        
        # Calculate combined statistics
        total_distance = sum(leg.total_distance for leg in track_legs)
        total_elevation_gain = sum(leg.elevation_gain for leg in track_legs)
        total_duration = None
        if all(leg.duration for leg in track_legs if leg.duration is not None):
            total_duration = sum(leg.duration for leg in track_legs if leg.duration is not None)
        
        logger.info(f"Combined track: {len(track_legs)} legs, {len(all_points)} total points")
        logger.info(f"Total distance: {total_distance:.2f}km, Total elevation: {total_elevation_gain:.0f}m")
        
        return {
            'type': 'multi_leg',
            'legs': track_legs,
            'all_points': all_points,
            'bounds': combined_bounds,
            'total_distance': total_distance,
            'total_duration': total_duration,
            'elevation_gain': total_elevation_gain,
            'leg_count': len(track_legs)
        }
    
    @staticmethod
    def parse_gpx_file(file_path: str) -> Dict:
        """Parse GPX file and extract track data"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            namespace = {'gpx': root.tag.split('}')[0][1:]} if '}' in root.tag else {}
            tracks = []
            
            track_elements = root.findall('.//gpx:trk', namespace) if namespace else root.findall('.//trk')
            
            for track_elem in track_elements:
                track_name = track_elem.find('.//gpx:name', namespace) if namespace else track_elem.find('.//name')
                track_name = track_name.text if track_name is not None else "Unnamed Track"
                
                points = []
                segments = track_elem.findall('.//gpx:trkseg', namespace) if namespace else track_elem.findall('.//trkseg')
                
                for segment in segments:
                    track_points = segment.findall('.//gpx:trkpt', namespace) if namespace else segment.findall('.//trkpt')
                    
                    for pt in track_points:
                        lat = float(pt.get('lat'))
                        lng = float(pt.get('lon'))
                        
                        ele_elem = pt.find('.//gpx:ele', namespace) if namespace else pt.find('.//ele')
                        elevation = float(ele_elem.text) if ele_elem is not None else None
                        
                        time_elem = pt.find('.//gpx:time', namespace) if namespace else pt.find('.//time')
                        timestamp = None
                        if time_elem is not None:
                            try:
                                timestamp = datetime.fromisoformat(time_elem.text.replace('Z', '+00:00'))
                            except:
                                pass
                        
                        points.append(GPSPoint(lat, lng, elevation, timestamp))
                
                if points:
                    tracks.append({
                        'name': track_name,
                        'points': points
                    })
            
            if not tracks:
                raise ValueError("No valid tracks found in GPX file")
            
            main_track = tracks[0]
            processed_data = GPXProcessor._process_track_data(main_track['points'])
            
            return {
                'name': main_track['name'],
                'points': main_track['points'],
                'bounds': processed_data['bounds'],
                'total_distance': processed_data['total_distance'],
                'duration': processed_data['duration'],
                'elevation_gain': processed_data['elevation_gain']
            }
            
        except Exception as e:
            logger.error(f"Error parsing GPX file: {str(e)}")
            raise ValueError(f"Invalid GPX file: {str(e)}")
    
    @staticmethod
    def _process_track_data(points: List[GPSPoint]) -> Dict:
        """Process track data and calculate cumulative distances"""
        if len(points) < 2:
            raise ValueError("Track must have at least 2 points")
        
        # Calculate bounds
        lats = [p.lat for p in points]
        lngs = [p.lng for p in points]
        bounds = TrackBounds(
            north=max(lats),
            south=min(lats),
            east=max(lngs),
            west=min(lngs)
        )
        
        # Calculate cumulative distances
        total_distance = 0.0
        points[0].distance_from_start = 0.0
        
        for i in range(1, len(points)):
            prev_point = points[i-1]
            curr_point = points[i]
            
            distance_km = GPXProcessor._haversine_distance(
                prev_point.lat, prev_point.lng,
                curr_point.lat, curr_point.lng
            )
            total_distance += distance_km
            curr_point.distance_from_start = total_distance
        
        # Calculate elevation gain
        elevation_gain = 0.0
        if all(p.elevation is not None for p in points):
            for i in range(1, len(points)):
                if points[i].elevation > points[i-1].elevation:
                    elevation_gain += points[i].elevation - points[i-1].elevation
        
        # Calculate duration
        duration = None
        if points[0].timestamp and points[-1].timestamp:
            duration = (points[-1].timestamp - points[0].timestamp).total_seconds()
        
        return {
            'bounds': bounds,
            'total_distance': total_distance,
            'duration': duration,
            'elevation_gain': elevation_gain
        }
    
    @staticmethod
    def interpolate_constant_speed_points(points: List[GPSPoint], target_frames: int) -> List[GPSPoint]:
        """Create evenly spaced points for constant playback speed"""
        if len(points) < 2:
            return points
            
        if target_frames <= len(points):
            # If we need fewer points, sample evenly
            indices = np.linspace(0, len(points) - 1, target_frames, dtype=int)
            return [points[i] for i in indices]
        
        # Calculate total distance
        total_distance = points[-1].distance_from_start
        distance_per_frame = total_distance / (target_frames - 1)
        
        interpolated_points = [points[0]]  # Start with first point
        
        current_distance = 0.0
        point_index = 0
        
        for frame in range(1, target_frames - 1):
            target_distance = frame * distance_per_frame
            
            # Find the segment containing this distance
            while (point_index < len(points) - 1 and 
                   points[point_index + 1].distance_from_start < target_distance):
                point_index += 1
            
            if point_index >= len(points) - 1:
                break
                
            # Interpolate between points[point_index] and points[point_index + 1]
            p1 = points[point_index]
            p2 = points[point_index + 1]
            
            segment_start_dist = p1.distance_from_start
            segment_end_dist = p2.distance_from_start
            segment_length = segment_end_dist - segment_start_dist
            
            if segment_length > 0:
                ratio = (target_distance - segment_start_dist) / segment_length
            else:
                ratio = 0
            
            # Linear interpolation
            lat = p1.lat + (p2.lat - p1.lat) * ratio
            lng = p1.lng + (p2.lng - p1.lng) * ratio
            elevation = None
            if p1.elevation is not None and p2.elevation is not None:
                elevation = p1.elevation + (p2.elevation - p1.elevation) * ratio
            
            interpolated_point = GPSPoint(lat, lng, elevation, None, target_distance)
            interpolated_points.append(interpolated_point)
        
        # Add the last point
        interpolated_points.append(points[-1])
        
        logger.info(f"Interpolated {len(points)} original points to {len(interpolated_points)} constant-speed points")
        return interpolated_points
    
    @staticmethod
    def _haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points using Haversine formula"""
        R = 6371  # Earth's radius in kilometers
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lng = math.radians(lng2 - lng1)
        
        a = (math.sin(delta_lat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * 
             math.sin(delta_lng / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c

class MapTileProvider:
    """Provides high-quality map tiles from various sources"""
    
    TILE_PROVIDERS = {
        'osm': 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
        'satellite': 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        'satellite_esri': 'https://clarity.maptiles.arcgis.com/arcgis/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        'terrain': 'https://stamen-tiles.a.ssl.fastly.net/terrain/{z}/{x}/{y}.png',
        'terrain_usgs': 'https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryTopo/MapServer/tile/{z}/{y}/{x}',
        'satellite_google': 'https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',  # Note: Check terms of use
        'hybrid_google': 'https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',    # Satellite + labels
        'mapbox_satellite': 'https://api.mapbox.com/v4/mapbox.satellite/{z}/{x}/{y}@2x.png?access_token={token}',  # Requires token
    }
    
    def __init__(self, cache_dir: str = None, mapbox_token: str = None):
        self.cache_dir = cache_dir or tempfile.gettempdir()
        self.mapbox_token = mapbox_token
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache for downloaded tiles
        self._tile_cache = {}
    
    def get_tile_url(self, x: int, y: int, z: int, style: str = 'satellite') -> str:
        """Get tile URL for given coordinates with improved providers"""
        if style not in self.TILE_PROVIDERS:
            logger.warning(f"Unknown tile style '{style}', falling back to satellite")
            style = 'satellite'
        
        url_template = self.TILE_PROVIDERS[style]
        
        # Handle Mapbox token if needed
        if 'mapbox' in style and self.mapbox_token:
            return url_template.format(x=x, y=y, z=z, token=self.mapbox_token)
        elif 'mapbox' in style:
            # Fallback to ESRI if no Mapbox token
            url_template = self.TILE_PROVIDERS['satellite']
        
        return url_template.format(x=x, y=y, z=z)
    
    def download_tile(self, x: int, y: int, z: int, style: str = 'satellite') -> Optional[Image.Image]:
        """Download a map tile with caching"""
        # Check cache first
        cache_key = f"{style}_{z}_{x}_{y}"
        if cache_key in self._tile_cache:
            return self._tile_cache[cache_key]
        
        try:
            # Check bounds to avoid invalid tile requests
            max_tile = 2 ** z
            if x < 0 or y < 0 or x >= max_tile or y >= max_tile:
                logger.debug(f"      Tile ({x},{y}) out of bounds for zoom {z}")
                return None
                
            url = self.get_tile_url(x, y, z, style)
            
            # Better user agent and headers for satellite imagery
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Referer': 'https://www.arcgis.com/'
            }
            
            logger.debug(f"      Fetching: {url}")
            response = requests.get(url, timeout=15, headers=headers)
            response.raise_for_status()
            
            tile_image = Image.open(BytesIO(response.content))
            
            # Ensure tile is 256x256
            if tile_image.size != (256, 256):
                tile_image = tile_image.resize((256, 256), Image.Resampling.LANCZOS)
            
            # Cache the tile
            self._tile_cache[cache_key] = tile_image
            
            logger.debug(f"      Successfully downloaded and cached tile ({x},{y})")
            return tile_image
            
        except requests.exceptions.Timeout:
            logger.warning(f"      Timeout downloading tile {x},{y},{z}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"      Network error downloading tile {x},{y},{z}: {str(e)}")
        except Exception as e:
            logger.warning(f"      Failed to download tile {x},{y},{z}: {str(e)}")
        
        # Return an improved placeholder tile
        logger.debug(f"      Creating enhanced placeholder for tile ({x},{y})")
        placeholder = Image.new('RGB', (256, 256), '#2D5016')  # Dark green for terrain
        draw = ImageDraw.Draw(placeholder)
        
        # Add a terrain-like pattern
        import random
        random.seed(x * 1000 + y)  # Consistent pattern per tile
        
        # Add some texture
        for _ in range(50):
            px = random.randint(0, 255)
            py = random.randint(0, 255)
            color_var = random.randint(-30, 30)
            color = (
                max(0, min(255, 45 + color_var)),
                max(0, min(255, 80 + color_var)),
                max(0, min(255, 22 + color_var))
            )
            draw.ellipse([px-2, py-2, px+2, py+2], fill=color)
        
        return placeholder
    
    @staticmethod
    def lat_lng_to_tile(lat: float, lng: float, zoom: int) -> Tuple[float, float]:
        """Convert lat/lng to tile coordinates (with fractional parts for precise positioning)"""
        lat_rad = math.radians(lat)
        n = 2.0 ** zoom
        x = (lng + 180.0) / 360.0 * n
        y = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n
        return x, y

class VideoGenerator:
    """Generates animated videos from GPS track data with constant speed"""
    
    def __init__(self, settings: VideoSettings = None):
        self.settings = settings or VideoSettings()
        self.tile_provider = MapTileProvider()
        
        # Cache for map images
        self._map_cache = {}
        self._base_map_image = None
        self._tile_grid = None
        
        # Load fonts
        try:
            self.font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
            self.font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
        except:
            try:
                self.font = ImageFont.truetype("arial.ttf", 16)
                self.font_small = ImageFont.truetype("arial.ttf", 14)
            except:
                self.font = ImageFont.load_default()
                self.font_small = ImageFont.load_default()
    
    def generate_video(self, track_data: Dict, output_path: str, 
                      progress_callback: Optional[Callable[[int], None]] = None) -> str:
        """Generate video with constant speed playback"""
        
        if not track_data:
            raise ValueError("Invalid track data")
        
        is_multi_leg = track_data.get('type') == 'multi_leg'
        
        if is_multi_leg:
            if not track_data.get('legs') or not track_data.get('all_points'):
                raise ValueError("Invalid multi-leg track data")
            original_points = track_data['all_points']
        else:
            if not track_data.get('points'):
                raise ValueError("Invalid single track data")
            original_points = track_data['points']
        
        if len(original_points) < 2:
            raise ValueError("Track must have at least 2 points")
        
        # Calculate target number of frames for constant speed
        target_frames = self.settings.fps * self.settings.duration
        
        # Interpolate points for constant speed
        if self.settings.constant_speed:
            logger.info("Creating constant-speed interpolation...")
            interpolated_points = GPXProcessor.interpolate_constant_speed_points(
                original_points, target_frames
            )
        else:
            interpolated_points = original_points
        
        # Update track data with interpolated points
        if is_multi_leg:
            track_data['interpolated_points'] = interpolated_points
        else:
            track_data['interpolated_points'] = interpolated_points
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp()
        frames_dir = os.path.join(temp_dir, 'frames')
        os.makedirs(frames_dir)
        
        try:
            total_frames = len(interpolated_points)
            actual_duration = total_frames / self.settings.fps
            
            logger.info(f"Generating {total_frames} frames for constant-speed playback...")
            logger.info(f"Video will be {actual_duration:.2f} seconds at {self.settings.fps} FPS")
            
            # Pre-generate base map
            logger.info("🗺️  Pre-generating high-quality satellite base map...")
            self._base_map_image = self._create_base_map_for_track(track_data, interpolated_points)
            
            logger.info(f"✅ Base map created! Now generating frames...")
            
            import time
            start_time = time.time()
            
            # Generate frames
            for frame_num in range(total_frames):
                if frame_num % 50 == 0 or frame_num < 5:
                    logger.info(f"Generating frame {frame_num + 1}/{total_frames}")
                
                frame_image = self._render_frame_fast(track_data, frame_num, interpolated_points)
                
                # Save frame
                frame_path = os.path.join(frames_dir, f"frame_{frame_num:06d}.png")
                frame_image.save(frame_path, "PNG", optimize=True)
                
                # Progress callback
                if progress_callback:
                    progress = int((frame_num / total_frames) * 80)  # 80% for frame generation
                    progress_callback(progress)
            
            # Compile video
            logger.info("Compiling video with FFmpeg...")
            self._compile_video(frames_dir, output_path, progress_callback)
            
            total_time = time.time() - start_time
            logger.info(f"Total video generation time: {total_time:.2f}s ({total_time/60:.1f} minutes)")
            logger.info(f"Video generated successfully: {output_path}")
            return output_path
            
        finally:
            # Cleanup
            shutil.rmtree(temp_dir, ignore_errors=True)
            self._map_cache.clear()
            self._base_map_image = None
    
    def _create_base_map_for_track(self, track_data: Dict, points: List[GPSPoint]) -> Image.Image:
        """Create high-quality satellite base map"""
        bounds = track_data['bounds']
        zoom_level = self._calculate_zoom_level(bounds)
        
        # Calculate tile coverage with buffer
        track_nw_x, track_nw_y = self.tile_provider.lat_lng_to_tile(bounds.north, bounds.west, zoom_level)
        track_se_x, track_se_y = self.tile_provider.lat_lng_to_tile(bounds.south, bounds.east, zoom_level)
        
        min_tile_x_track = int(min(track_nw_x, track_se_x))
        max_tile_x_track = int(max(track_nw_x, track_se_x))
        min_tile_y_track = int(min(track_nw_y, track_se_y))
        max_tile_y_track = int(max(track_nw_y, track_se_y))
        
        # Larger buffer for smoother panning
        buffer_tiles = 3
        start_x = min_tile_x_track - buffer_tiles
        end_x = max_tile_x_track + buffer_tiles + 1
        start_y = min_tile_y_track - buffer_tiles
        end_y = max_tile_y_track + buffer_tiles + 1
        
        tiles_x = end_x - start_x
        tiles_y = end_y - start_y
        total_tiles = tiles_x * tiles_y
        
        logger.info(f"  Creating satellite base map: {tiles_x}x{tiles_y} tiles ({total_tiles} total) at zoom {zoom_level}")
        
        map_width = tiles_x * 256
        map_height = tiles_y * 256
        base_map = Image.new('RGB', (map_width, map_height), self.settings.background_color)
        
        # Download tiles with progress
        tile_count = 0
        for tx in range(start_x, end_x):
            for ty in range(start_y, end_y):
                tile_count += 1
                if tile_count % 20 == 0:
                    logger.info(f"    Downloaded {tile_count}/{total_tiles} tiles ({(tile_count/total_tiles*100):.0f}%)")
                
                try:
                    tile_img = self.tile_provider.download_tile(tx, ty, zoom_level, self.settings.map_style)
                    if tile_img:
                        pos_x = (tx - start_x) * 256
                        pos_y = (ty - start_y) * 256
                        base_map.paste(tile_img, (pos_x, pos_y))
                except Exception as e:
                    logger.debug(f"      Error loading tile {tx},{ty}: {e}")
        
        # Store tile grid info
        self._tile_grid = {
            'start_x': start_x,
            'start_y': start_y,
            'tiles_x': tiles_x,
            'tiles_y': tiles_y,
            'zoom': zoom_level,
            'map_width': map_width,
            'map_height': map_height
        }
        
        logger.info(f"  High-quality satellite base map created: {map_width}x{map_height} pixels")
        return base_map
    
    def _render_frame_fast(self, track_data: Dict, frame_index: int, points: List[GPSPoint]) -> Image.Image:
        """Render frame with constant speed - simplified without speedometer"""
        current_point = points[frame_index]
        visible_points = points[:frame_index + 1]
        
        # Get current position in tile coordinates
        current_tile_x, current_tile_y = self.tile_provider.lat_lng_to_tile(
            current_point.lat, current_point.lng, self._tile_grid['zoom']
        )
        
        # Calculate crop area from base map
        grid = self._tile_grid
        pixel_x = (current_tile_x - grid['start_x']) * 256
        pixel_y = (current_tile_y - grid['start_y']) * 256
        
        crop_x = int(pixel_x - self.settings.width // 2)
        crop_y = int(pixel_y - self.settings.height // 2)
        
        # Keep crop within bounds
        crop_x = max(0, min(crop_x, grid['map_width'] - self.settings.width))
        crop_y = max(0, min(crop_y, grid['map_height'] - self.settings.height))
        
        # Crop and copy the base map
        frame_img = self._base_map_image.crop((
            crop_x, crop_y, 
            crop_x + self.settings.width,
            crop_y + self.settings.height
        )).copy()
        
        draw = ImageDraw.Draw(frame_img)
        
        # Projection function
        def project_to_screen_fast(point: GPSPoint) -> Tuple[int, int]:
            point_tile_x, point_tile_y = self.tile_provider.lat_lng_to_tile(
                point.lat, point.lng, grid['zoom']
            )
            base_pixel_x = (point_tile_x - grid['start_x']) * 256
            base_pixel_y = (point_tile_y - grid['start_y']) * 256
            
            frame_x = int(base_pixel_x - crop_x)
            frame_y = int(base_pixel_y - crop_y)
            
            return frame_x, frame_y
        
        # Draw track trail
        if self.settings.show_trail and len(visible_points) > 1:
            is_multi_leg = track_data.get('type') == 'multi_leg'
            if is_multi_leg:
                self._draw_multi_leg_track_interpolated(draw, track_data, frame_index, points, project_to_screen_fast)
            else:
                self._draw_track_simple(draw, visible_points, project_to_screen_fast)
        
        # Draw current position marker
        self._draw_marker(draw, current_point, project_to_screen_fast)
        
        # Draw simplified data overlay (no speedometer)
        if self.settings.show_data_overlay:
            self._draw_simplified_data_overlay(draw, track_data, frame_index, points, frame_img.size)
        
        # Draw progress bar
        self._draw_progress_bar(draw, frame_index, len(points), frame_img.size)
        
        return frame_img
    
    def _draw_multi_leg_track_interpolated(self, draw: ImageDraw.Draw, track_data: Dict, 
                                         current_frame: int, interpolated_points: List[GPSPoint], 
                                         project_func):
        """Draw multi-leg track for interpolated constant-speed points"""
        legs = track_data['legs']
        if not legs:
            return
        
        # For interpolated points, we need to determine which leg each point belongs to
        # by mapping back to original track structure
        original_points = track_data['all_points']
        visible_interpolated = interpolated_points[:current_frame + 1]
        
        if len(visible_interpolated) < 2:
            return
        
        # Simple approach: draw all visible points with a single color for now
        # In a more sophisticated version, we'd map each interpolated point back to its source leg
        screen_points = [project_func(p) for p in visible_interpolated]
        
        # Draw track segments
        for i in range(1, len(screen_points)):
            start = screen_points[i-1]
            end = screen_points[i]
            draw.line([start, end], fill=self.settings.track_color, width=self.settings.track_width)
    
    def _draw_track_simple(self, draw: ImageDraw.Draw, points: List[GPSPoint], project_func):
        """Draw simple track without speed coloring"""
        if len(points) < 2:
            return
        
        screen_points = [project_func(p) for p in points]
        
        for i in range(1, len(screen_points)):
            start = screen_points[i-1]
            end = screen_points[i]
            draw.line([start, end], fill=self.settings.track_color, width=self.settings.track_width)
    
    def _draw_marker(self, draw: ImageDraw.Draw, point: GPSPoint, project_func):
        """Draw current position marker"""
        x, y = project_func(point)
        size = self.settings.marker_size
        
        # Enhanced marker with better visibility
        draw.ellipse([x-size-2, y-size-2, x+size+2, y+size+2], fill="white")
        draw.ellipse([x-size, y-size, x+size, y+size], fill=self.settings.marker_color)
        # Add inner dot for better visibility
        inner_size = size // 2
        draw.ellipse([x-inner_size, y-inner_size, x+inner_size, y+inner_size], fill="white")
    
    def _draw_simplified_data_overlay(self, draw: ImageDraw.Draw, track_data: Dict, 
                                    frame_index: int, points: List[GPSPoint], size: Tuple[int, int]):
        """Draw simplified data overlay without speedometer"""
        width, height = size
        is_multi_leg = track_data.get('type') == 'multi_leg'
        
        # Smaller panel since no speedometer
        panel_width = 280
        panel_height = 100 if not is_multi_leg else 120
        panel_x = width - panel_width - 20
        panel_y = 20
        
        # Draw panel background with better styling
        draw.rectangle([panel_x, panel_y, panel_x + panel_width, panel_y + panel_height], 
                      fill=(0, 0, 0, 180), outline="white", width=2)
        
        # Current point data
        current_point = points[frame_index]
        elevation = current_point.elevation or 0
        progress = (frame_index / len(points)) * 100 if len(points) > 1 else 0
        
        # Calculate current distance
        current_distance = current_point.distance_from_start if hasattr(current_point, 'distance_from_start') else 0
        
        y_offset = panel_y + 15
        
        if is_multi_leg:
            # Multi-leg info
            draw.text((panel_x + 15, y_offset), f"Multi-leg Trip ({track_data['leg_count']} legs)", 
                     fill="white", font=self.font)
            y_offset += 25
            draw.text((panel_x + 15, y_offset), f"Distance: {current_distance:.2f} / {track_data['total_distance']:.2f} km", 
                     fill="lightblue", font=self.font_small)
            y_offset += 20
        else:
            draw.text((panel_x + 15, y_offset), "GPS Track Progress", fill="white", font=self.font)
            y_offset += 25
            draw.text((panel_x + 15, y_offset), f"Distance: {current_distance:.2f} km", 
                     fill="lightblue", font=self.font_small)
            y_offset += 20
        
        draw.text((panel_x + 15, y_offset), f"Elevation: {elevation:.0f}m", fill="lightgreen", font=self.font_small)
        y_offset += 20
        draw.text((panel_x + 15, y_offset), f"Progress: {progress:.1f}%", fill="yellow", font=self.font_small)
    
    def _draw_progress_bar(self, draw: ImageDraw.Draw, current_index: int, total_points: int, size: Tuple[int, int]):
        """Draw enhanced progress bar"""
        width, height = size
        
        bar_width = width - 40
        bar_height = 10
        bar_x = 20
        bar_y = height - 35
        
        # Background with rounded corners effect
        draw.rectangle([bar_x, bar_y, bar_x + bar_width, bar_y + bar_height], 
                      fill=(100, 100, 100), outline="white", width=1)
        
        # Progress
        if total_points > 1:
            progress = current_index / (total_points - 1)
            progress_width = int(bar_width * progress)
            
            # Gradient-like effect with multiple colors
            if progress_width > 0:
                # Green progress bar
                draw.rectangle([bar_x, bar_y, bar_x + progress_width, bar_y + bar_height], 
                              fill=(50, 200, 50))
                
                # Add percentage text
                percentage = progress * 100
                text = f"{percentage:.1f}%"
                try:
                    bbox = draw.textbbox((0, 0), text, font=self.font_small)
                    text_width = bbox[2] - bbox[0]
                    text_x = bar_x + bar_width + 10
                    text_y = bar_y - 5
                    draw.text((text_x, text_y), text, fill="white", font=self.font_small)
                except:
                    # Fallback for older PIL versions
                    text_x = bar_x + bar_width + 10
                    text_y = bar_y - 5
                    draw.text((text_x, text_y), text, fill="white", font=self.font_small)
    
    def _calculate_zoom_level(self, bounds: TrackBounds) -> int:
        """Calculate appropriate zoom level for track bounds"""
        padding = 0.15  # Reduced padding for better view
        
        lat_span = bounds.north - bounds.south
        lng_span = bounds.east - bounds.west
        
        lat_span *= (1 + 2 * padding)
        lng_span *= (1 + 2 * padding)
        
        min_zoom = 6
        max_zoom = 17  # Increased for better satellite detail
        best_zoom = min_zoom
        
        for z in range(min_zoom, max_zoom + 1):
            nw_x, nw_y = self.tile_provider.lat_lng_to_tile(
                bounds.north + lat_span/2, bounds.west - lng_span/2, z)
            se_x, se_y = self.tile_provider.lat_lng_to_tile(
                bounds.south - lat_span/2, bounds.east + lng_span/2, z)
            
            required_pixel_width = abs(nw_x - se_x) * 256
            required_pixel_height = abs(nw_y - se_y) * 256
            
            if (required_pixel_width <= self.settings.width and 
                required_pixel_height <= self.settings.height):
                best_zoom = z
            else:
                if z > min_zoom:
                    best_zoom = z - 1
                break
        
        return max(min_zoom, best_zoom)
    
    def _compile_video(self, frames_dir: str, output_path: str, 
                      progress_callback: Optional[Callable[[int], None]] = None):
        """Compile frames into video using multiple fallback methods"""
        
        # Method 1: Try imageio (bundled FFmpeg)
        try:
            import imageio
            
            frame_files = sorted([f for f in os.listdir(frames_dir) if f.endswith('.png')])
            frame_paths = [os.path.join(frames_dir, f) for f in frame_files]
            
            if not frame_paths:
                raise RuntimeError("No frames found to compile")
            
            # High quality settings for satellite imagery
            with imageio.get_writer(output_path, fps=self.settings.fps, 
                                  codec='libx264', bitrate='8M', quality=8) as writer:
                for i, frame_path in enumerate(frame_paths):
                    image = imageio.imread(frame_path)
                    writer.append_data(image)
                    
                    if progress_callback and i % 10 == 0:
                        progress = 80 + int((i / len(frame_paths)) * 20)
                        progress_callback(progress)
            
            if progress_callback:
                progress_callback(100)
            
            logger.info("Video compiled successfully using imageio with high quality settings")
            return
            
        except ImportError:
            logger.warning("imageio not available, trying OpenCV...")
        except Exception as e:
            logger.warning(f"imageio failed: {str(e)}, trying OpenCV...")
        
        # Method 2: Try OpenCV
        try:
            import cv2
            
            frame_files = sorted([f for f in os.listdir(frames_dir) if f.endswith('.png')])
            if not frame_files:
                raise RuntimeError("No frames found to compile")
            
            first_frame = cv2.imread(os.path.join(frames_dir, frame_files[0]))
            height, width, _ = first_frame.shape
            
            # High quality codec settings
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            out = cv2.VideoWriter(output_path, fourcc, float(self.settings.fps), (width, height))
            
            for i, frame_file in enumerate(frame_files):
                frame_path = os.path.join(frames_dir, frame_file)
                frame = cv2.imread(frame_path)
                out.write(frame)
                
                if progress_callback and i % 10 == 0:
                    progress = 80 + int((i / len(frame_files)) * 20)
                    progress_callback(progress)
            
            out.release()
            
            if progress_callback:
                progress_callback(100)
            
            logger.info("Video compiled successfully using OpenCV")
            return
            
        except ImportError:
            logger.warning("OpenCV not available, trying system FFmpeg...")
        except Exception as e:
            logger.warning(f"OpenCV failed: {str(e)}, trying system FFmpeg...")
        
        # Method 3: Fallback to system FFmpeg with high quality
        try:
            subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
            
            cmd = [
                'ffmpeg', '-y',
                '-framerate', str(self.settings.fps),
                '-i', os.path.join(frames_dir, 'frame_%06d.png'),
                '-c:v', 'libx264',
                '-pix_fmt', 'yuv420p',
                '-crf', '18',  # Higher quality for satellite imagery
                '-preset', 'medium',
                output_path
            ]
            
            process = subprocess.run(cmd, capture_output=True, text=True)
            
            if process.returncode != 0:
                raise RuntimeError(f"FFmpeg failed: {process.stderr}")
            
            if progress_callback:
                progress_callback(100)
            
            logger.info("Video compiled successfully using system FFmpeg with high quality settings")
            return
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        
        raise RuntimeError(
            "No video compilation method available. Please install one of: "
            "imageio[ffmpeg], opencv-python, or system FFmpeg"
        )

def generate_gps_video(gpx_files, output_path: str, 
                      settings: Dict = None, 
                      progress_callback: Optional[Callable[[int], None]] = None) -> str:
    """
    Main function to generate GPS video with Phase 1 improvements
    
    Args:
        gpx_files: GPX file(s) - string path, list of paths, or list of dicts
        output_path: Output video path
        settings: Video settings dict
        progress_callback: Progress callback function
    
    Returns:
        Path to generated video
    """
    
    try:
        # Parse settings with new defaults
        video_settings = VideoSettings()
        if settings:
            for key, value in settings.items():
                if hasattr(video_settings, key):
                    setattr(video_settings, key, value)
        
        # Handle input types
        if isinstance(gpx_files, str):
            logger.info(f"Processing single GPX file: {gpx_files}")
            track_data = GPXProcessor.parse_gpx_file(gpx_files)
        elif isinstance(gpx_files, list):
            if len(gpx_files) == 1:
                file_info = gpx_files[0]
                if isinstance(file_info, str):
                    track_data = GPXProcessor.parse_gpx_file(file_info)
                else:
                    track_data = GPXProcessor.parse_gpx_file(file_info['path'])
            else:
                logger.info(f"Processing {len(gpx_files)} GPX files for multi-leg trip")
                
                processed_files = []
                for i, file_info in enumerate(gpx_files):
                    if isinstance(file_info, str):
                        processed_files.append({
                            'path': file_info,
                            'name': f"Leg {i+1}",
                            'color': video_settings.default_colors[i % len(video_settings.default_colors)]
                        })
                    else:
                        processed_files.append(file_info)
                
                track_data = GPXProcessor.parse_multiple_gpx_files(processed_files)
        else:
            raise ValueError("gpx_files must be a string path or list of paths/dicts")
        
        # Generate video with improvements
        logger.info(f"Generating high-quality satellite video: {output_path}")
        generator = VideoGenerator(video_settings)
        result_path = generator.generate_video(track_data, output_path, progress_callback)
        
        logger.info("✅ Phase 1 video generation completed successfully!")
        logger.info("🎬 Improvements: Constant speed playback, high-quality satellite imagery, no speedometer")
        return result_path
        
    except Exception as e:
        logger.error(f"Video generation failed: {str(e)}")
        raise

# Example usage with Phase 1 improvements
if __name__ == "__main__":
    
    # Phase 1 settings - constant speed, satellite imagery, no speedometer
    phase1_settings = {
        'width': 1920,
        'height': 1080,
        'fps': 30,
        'duration': 30,  # 30 seconds of constant speed playback
        'map_style': 'satellite',  # High-quality satellite imagery
        'show_data_overlay': True,
        'show_trail': True,
        'track_width': 6,
        'constant_speed': True,  # Key improvement
        'track_color': '#FF3333',
        'marker_color': '#FFFF00',
        'marker_size': 14
    }
    
    # Single track example
    # generate_gps_video(
    #     gpx_files="track.gpx",
    #     output_path="satellite_track_constant_speed.mp4",
    #     settings=phase1_settings
    # )
    
    # Multi-leg example with custom colors
    multi_leg_example = [
        {'path': '1.gpx', 'name': 'Ferry Ride', 'color': "#0066CC"},
        {'path': '2.gpx', 'name': 'Mountain Hike', 'color': "#009900"},
        {'path': '3.gpx', 'name': 'Coastal Drive', 'color': "#FF6600"},
    ]
    
    generate_gps_video(
        gpx_files=multi_leg_example,
        output_path="multi_leg_satellite_constant_speed.mp4",
        settings=phase1_settings
    )
    
    print("Phase 1 improvements ready!")
    print("✅ Removed speedometer")
    print("✅ Constant speed interpolation")
    print("✅ High-quality satellite imagery")
    print("✅ Enhanced visual styling")
    print("🚀 Ready for Phase 2: 3D terrain integration!")