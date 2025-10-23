from flask import Blueprint, jsonify, request, abort, render_template, session
from shapely.geometry import shape
from src.pg import pg_session
from src.sql.completion import get_admin_areas_by_level, get_coverage_units, delete_coverage_units, merge_coverage_units, update_admin_area_geom
from src.api.completion_helpers import organize_admin_areas_by_continent
from src.utils import getUser, isCurrentTrip, lang

completion_blueprint = Blueprint('completion', __name__)

def get_country_codes_from_db(pg):
    countries = pg.execute(get_admin_areas_by_level(), {"level": 1}).fetchall()
    regions = pg.execute(get_admin_areas_by_level(), {"level": 2}).fetchall()
    countries_list = [{"iso_code": r[0], "name": r[1], "level": r[2], "parent_iso": r[3]} for r in countries]
    regions_list = [{"iso_code": r[0], "name": r[1], "level": r[2], "parent_iso": r[3]} for r in regions]
    return organize_admin_areas_by_continent(countries_list, regions_list)

@completion_blueprint.route("/admin/editCountriesList")
def edit_countries_list():
    userinfo = session.get("userinfo", {})
    with pg_session() as pg:
        country_data = get_country_codes_from_db(pg)
    def get_country_codes_from_files():
        return country_data
    return render_template("admin/edit_coverage_list.html", title="Edit List", username=getUser(), nav="bootstrap/navigation.html", get_country_codes_from_files=get_country_codes_from_files, isCurrent=isCurrentTrip(getUser()), **lang[userinfo.get("lang", "en")], **userinfo)

@completion_blueprint.route("/admin/editCountries/<cc>")
def edit_countries(cc):
    userinfo = session.get("userinfo", {})
    with pg_session() as pg:
        result = pg.execute(get_coverage_units(), {"iso_code": cc}).fetchone()
        if not result or not result[0]:
            abort(410)
    return render_template("admin/country_edit.html", title=f"Edit {cc}", username=getUser(), nav="bootstrap/navigation.html", cc=cc, isCurrent=isCurrentTrip(getUser()), **lang[userinfo.get("lang", "en")], **userinfo)

@completion_blueprint.route("/getGeojson/<cc>", methods=["GET"])
def get_full_geojson(cc):
    with pg_session() as pg:
        result = pg.execute(get_coverage_units(), {"iso_code": cc}).fetchone()
        if not result or not result[0]:
            abort(404)
        return jsonify(result[0])

@completion_blueprint.route("/processQueue/<cc>", methods=["POST"])
def process_queue(cc):
    try:
        operations = request.json
        if not operations or len(operations) == 0:
            return jsonify({"success": False, "message": "No operations to process"})
        with pg_session() as pg:
            for operation in operations:
                operation_type = operation["type"]
                polygon_ids = [int(pid) for pid in operation["polygonIds"]]
                if operation_type == "delete":
                    pg.execute(delete_coverage_units(), {"unit_ids": polygon_ids})
                elif operation_type == "merge":
                    if len(polygon_ids) != 2:
                        return jsonify({"success": False, "message": f"Merge requires exactly 2 polygons, got {len(polygon_ids)}"})
                    units_result = pg.execute(get_coverage_units(), {"iso_code": cc}).fetchone()
                    if not units_result:
                        return jsonify({"success": False, "message": f"Could not load data for {cc}"})
                    geojson = units_result[0]
                    features = geojson.get('features', [])
                    polys_to_merge = [f for f in features if f['properties']['id'] in polygon_ids]
                    if len(polys_to_merge) != 2:
                        return jsonify({"success": False, "message": f"Could not find both polygons (found {len(polys_to_merge)})"})
                    shape1 = shape(polys_to_merge[0]['geometry'])
                    shape2 = shape(polys_to_merge[1]['geometry'])
                    if not shape1.buffer(0.0001).intersects(shape2):
                        return jsonify({"success": False, "message": "Selected polygons are not contiguous and cannot be merged"})
                    pg.execute(merge_coverage_units(), {"unit_ids": polygon_ids})
                else:
                    return jsonify({"success": False, "message": f"Unknown operation type: {operation_type}"})
            pg.execute(update_admin_area_geom(), {"iso_code": cc})
        return jsonify({"success": True, "message": f"Successfully processed {len(operations)} operation{'s' if len(operations) > 1 else ''}"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error processing operations: {str(e)}"})