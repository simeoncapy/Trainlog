CONTINENT_MAP = {
    'EU': ['AT','BE','BG','HR','CY','CZ','DK','EE','FI','FR','DE','GR','HU','IE','IT','LV','LT','LU','MT','NL','PL','PT','RO','SK','SI','ES','SE','GB','NO','CH','IS','LI','MC','SM','VA','AD','ME','RS','BA','MK','AL','XK'],
    'AF': ['DZ','AO','BJ','BW','BF','BI','CM','CV','CF','TD','KM','CG','CD','CI','DJ','EG','GQ','ER','ET','GA','GM','GH','GN','GW','KE','LS','LR','LY','MG','MW','ML','MR','MU','YT','MA','MZ','NA','NE','NG','RE','RW','SH','ST','SN','SC','SL','SO','ZA','SS','SD','SZ','TZ','TG','TN','UG','ZM','ZW'],
    'AS': ['AF','AM','AZ','BH','BD','BT','BN','KH','CN','GE','HK','IN','ID','IR','IQ','IL','JP','JO','KZ','KW','KG','LA','LB','MO','MY','MV','MN','MM','NP','KP','OM','PK','PS','PH','QA','SA','SG','KR','LK','SY','TW','TJ','TH','TL','TR','TM','AE','UZ','VN','YE'],
    'NA': ['CA','US','MX'],
    'CA': ['BZ','CR','SV','GT','HN','NI','PA'],
    'SA': ['AR','BO','BR','CL','CO','EC','FK','GF','GY','PY','PE','SR','UY','VE'],
    'OC': ['AS','AU','CK','FJ','PF','GU','KI','MH','FM','NR','NC','NZ','NU','NF','MP','PW','PG','PN','WS','SB','TK','TO','TV','VU','WF']
}

def get_continent_for_iso(iso_code):
    for continent, countries in CONTINENT_MAP.items():
        if iso_code in countries:
            return continent
    return None

def organize_admin_areas_by_continent(countries, regions):
    result = {'EU':[],'AF':[],'AS':[],'NA':[],'CA':[],'SA':[],'OC':[]}
    for country in countries:
        iso = country['iso_code']
        continent = get_continent_for_iso(iso)
        if continent:
            result[continent].append(iso)
    for region in regions:
        parent = region['parent_iso']
        region_key = f"Region_{parent}"
        if region_key not in result:
            result[region_key] = []
        result[region_key].append(region['iso_code'])
    return {k:v for k,v in result.items() if v}
