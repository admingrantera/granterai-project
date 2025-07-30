-- scoring_functions.sql

-- Function to calculate the Geographic Proximity Score
-- This function returns a score based on whether the foundation's state matches the charity's state.
-- A higher score is given for a closer geographic match.

CREATE OR REPLACE FUNCTION calculate_geo_score(
    charity_state TEXT,
    foundation_state TEXT
)
RETURNS INTEGER AS $$
BEGIN
    -- 100 points if states match (same city check will be handled in the main query)
    IF charity_state = foundation_state THEN
        RETURN 100;
    -- We can expand this with regional logic later if needed
    -- For now, any other case gets a base score
    ELSE
        RETURN 30;
    END IF;
END;
$$ LANGUAGE plpgsql;
