create or replace task F1_ANALYTICS.RAW.LOAD_RESULTS_TO_GOLD
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 23 * * SUN UTC'
	as MERGE INTO F1_ANALYTICS.GOLD.QUALIFICATION_RESULTS TGT
    USING (
        SELECT 
             SESSION_KEY
            ,POSITION
            ,NUMBER_OF_LAPS
            ,DNF
            ,DNS
            ,DSQ
            ,DRIVER_NUMBER
            ,Q1_TIMING
            ,Q2_TIMING
            ,Q3_TIMING
            ,CASE WHEN Q1_TIMING IS NULL THEN 0 ELSE 1 END AS HAS_Q1
            ,CASE WHEN Q2_TIMING IS NULL THEN 0 ELSE 1 END AS HAS_Q2
            ,CASE WHEN Q3_TIMING IS NULL THEN 0 ELSE 1 END AS HAS_Q3
            ,(
                (
                COALESCE(Q1_TIMING, 0)+ COALESCE(Q2_TIMING, 0) + COALESCE(Q3_TIMING, 0)
                )
                /
                (
                COALESCE(HAS_Q1, 0)+COALESCE(HAS_Q2, 0)+COALESCE(HAS_Q3, 0)
                )
                )::DECIMAL(10,3) 
                AS AVG_QUALI_TIME
            ,LEAST(
                COALESCE(Q1_TIMING, 999)
                ,COALESCE(Q2_TIMING, 999)
                ,COALESCE(Q3_TIMING, 999)
                ) AS FASTEST_QUALIFYING_TIME
            ,CASE WHEN POSITION <= 3 THEN 1 ELSE 0 END AS WINNER_FLAG
            
        FROM F1_ANALYTICS.SILVER.QUALIFYING_RESULTS
    )SRC
    ON SRC.SESSION_KEY = TGT.SESSION_KEY
    WHEN NOT MATCHED THEN
    INSERT(
             SESSION_KEY
            ,POSITION
            ,NUMBER_OF_LAPS
            ,DNF
            ,DNS
            ,DSQ
            ,DRIVER_NUMBER
            ,Q1_TIMING
            ,Q2_TIMING
            ,Q3_TIMING        
            ,HAS_Q1
            ,HAS_Q2
            ,HAS_Q3
            ,AVG_QUALI_TIME
            ,FASTEST_QUALIFYING_TIME
            ,WINNER_FLAG
            
    )
    VALUES(
             SRC.SESSION_KEY
            ,SRC.POSITION
            ,SRC.NUMBER_OF_LAPS
            ,SRC.DNF
            ,SRC.DNS
            ,SRC.DSQ
            ,SRC.DRIVER_NUMBER
            ,SRC.Q1_TIMING
            ,SRC.Q2_TIMING
            ,SRC.Q3_TIMING        
            ,SRC.HAS_Q1
            ,SRC.HAS_Q2
            ,SRC.HAS_Q3
            ,SRC.AVG_QUALI_TIME
            ,SRC.FASTEST_QUALIFYING_TIME
            ,SRC.WINNER_FALG
    );
