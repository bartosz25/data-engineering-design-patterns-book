CREATE SCHEMA dedp;

CREATE TABLE dedp.visits (
    visit_id VARCHAR(10) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id VARCHAR(25) NOT NULL,
    page VARCHAR(25) NOT NULL,
    ip VARCHAR(25) NOT NULL,
    login VARCHAR(25) NOT NULL,
    is_connected BOOL NOT NULL DEFAULT false,
    from_page VARCHAR(25) NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

CREATE FUNCTION disallow_removing_page() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_event_trigger_dropped_objects() AS dropped_objects
    WHERE
      dropped_objects.object_type = 'table column' AND
      dropped_objects.object_identity = 'dedp.visits.page')
  THEN
    RAISE EXCEPTION 'Cannot remove page from dedp.visits!';
  END IF;
END $$;

CREATE EVENT TRIGGER page_removal ON sql_drop EXECUTE PROCEDURE disallow_removing_page();
