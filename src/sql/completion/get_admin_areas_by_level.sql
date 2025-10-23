SELECT 
    a.iso_code,
    a.name,
    a.level,
    COALESCE(p.iso_code, '') as parent_iso
FROM admin_area a
LEFT JOIN admin_area p ON a.parent_admin_area_id = p.admin_area_id
WHERE a.level = :level
ORDER BY COALESCE(p.iso_code, ''), a.iso_code;
