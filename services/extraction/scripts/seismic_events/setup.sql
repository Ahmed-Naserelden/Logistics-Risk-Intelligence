CREATE SEQUENCE snowflake_earthquake_pk_seq;


ALTER TABLE seismic_events
    ADD COLUMN IF NOT EXISTS current_snowflake_pk BIGINT DEFAULT nextval('snowflake_earthquake_pk_seq'),
    ADD COLUMN IF NOT EXISTS last_snowflake_pk BIGINT;


CREATE OR REPLACE FUNCTION public.do_it_on_update_seismic_events()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.last_snowflake_pk := OLD.current_snowflake_pk;
    NEW.current_snowflake_pk := nextval('snowflake_earthquake_pk_seq');
    RETURN NEW;
END;
$function$  

CREATE OR REPLACE TRIGGER set_lastupdate_before_update
BEFORE UPDATE ON seismic_events
FOR EACH ROW
EXECUTE FUNCTION do_it_on_update_seismic_events();


-- ALTER TABLE seismic_events
-- DROP COLUMN IF EXISTS current_snowflake_pk,
-- DROP COLUMN IF EXISTS last_snowflake_pk;

SELECT *
FROM seismic_events;