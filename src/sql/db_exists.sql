SELECT EXISTS (
    SELECT *
    FROM information_schema.tables
    WHERE table_schema = 'meta' AND table_name = 'migrations'
);
