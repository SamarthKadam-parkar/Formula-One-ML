create or replace task F1_ANALYTICS.RAW.LOAD_RESULTS_TO_BRONZE
	warehouse=COMPUTE_WH
	schedule='USING CRON 30 22 * * SAT UTC'
	as MERGE INTO F1_ANALYTICS.BRONZE.RESULTS TGT
    USING(
        SELECT
     RAW_LOGS : session_key :: NUMBER AS SESSION_KEY
    ,RAW_LOGS : meeting_key :: NUMBER AS MEETING_KEY 
    ,RAW_LOGS : dnf :: boolean AS DNF
    ,RAW_LOGS : dns :: boolean AS DNS
    ,RAW_LOGS : dsq :: boolean AS DSQ
    ,RAW_LOGS : driver_number :: text AS DRIVER_NUMBER
    ,RAW_LOGS : duration AS DURATION
    ,RAW_LOGS : gap_to_leader AS GAP_TO_LEADER
    ,RAW_LOGS : position :: number AS POSITION
    ,RAW_LOGS : number_of_laps :: number AS NUMBER_OF_LAPS
    FROM F1_ANALYTICS.RAW.RESULTS
    ) SRC
    ON SRC.SESSION_KEY = TGT.SESSION_KEY
    WHEN NOT MATCHED THEN
    INSERT(  SESSION_KEY   
            ,MEETING_KEY   
            ,DNF           
            ,DNS           
            ,DSQ           
            ,DRIVER_NUMBER 
            ,DURATION      
            ,GAP_TO_LEADER 
            ,POSITION      
            ,NUMBER_OF_LAPS
            ) 
            VALUES(
                 SRC.SESSION_KEY   
                ,SRC.MEETING_KEY   
                ,SRC.DNF           
                ,SRC.DNS           
                ,SRC.DSQ           
                ,SRC.DRIVER_NUMBER 
                ,SRC.DURATION      
                ,SRC.GAP_TO_LEADER 
                ,SRC.POSITION      
                ,SRC.NUMBER_OF_LAPS
            );
