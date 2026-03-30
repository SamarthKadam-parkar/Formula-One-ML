create or replace task F1_ANALYTICS.RAW.LOAD_RESULTS_TO_SILVER
	warehouse=COMPUTE_WH
	schedule='USING CRON 30 22 * * SAT UTC'
	as MERGE INTO F1_ANALYTICS.SILVER.QUALIFYING_RESULTS TGT
    USING(
        SELECT 
                 R.SESSION_KEY
                ,R.MEETING_KEY
                ,S.CIRCUIT_KEY
                ,S.CIRCUIT_SHORT_NAME
                ,S.COUNTRY_CODE
                ,DNF
                ,DNS
                ,DSQ 
                ,D.DRIVER_NUMBER
                ,D.FULL_NAME
                ,R.DURATION[0]::FLOAT AS Q1_TIMING
                ,R.DURATION[1]::FLOAT AS Q2_TIMING
                ,R.DURATION[2]::FLOAT AS Q3_TIMING
                ,R.GAP_TO_LEADER[0]::FLOAT AS Q1_GLT
                ,R.GAP_TO_LEADER[1]::FLOAT AS Q2_GLT
                ,R.GAP_TO_LEADER[2]::FLOAT AS Q3_GLT
                ,R.POSITION
                ,R.NUMBER_OF_LAPS
            FROM F1_ANALYTICS.BRONZE.RESULTS R
            JOIN F1_ANALYTICS.BRONZE.SESSIONS S 
            ON S.SESSION_KEY = R.SESSION_KEY
            JOIN F1_ANALYTICS.SILVER.DRIVERS D 
            ON D.DRIVER_NUMBER = R.driver_number 
            HAVING S.SESSSION_TYPE = 'Qualifying' AND S.SESSION_NAME = 'Qualifying'
    ) SRC
    ON SRC.SESSION_KEY = TGT.SESSION_KEY
    WHEN NOT MATCHED THEN
    INSERT ( SESSION_KEY       
            ,MEETING_KEY       
            ,CIRCUIT_KEY       
            ,CIRCUIT_SHORT_NAME
            ,COUNTRY_CODE      
            ,DNF               
            ,DNS               
            ,DSQ               
            ,DRIVER_NUMBER     
            ,FULL_NAME         
            ,Q1_TIMING         
            ,Q2_TIMING         
            ,Q3_TIMING         
            ,Q1_GLT            
            ,Q2_GLT            
            ,Q3_GLT            
            ,POSITION          
            ,NUMBER_OF_LAPS) 
            VALUES(
             SRC.SESSION_KEY       
            ,SRC.MEETING_KEY       
            ,SRC.CIRCUIT_KEY       
            ,SRC.CIRCUIT_SHORT_NAME
            ,SRC.COUNTRY_CODE      
            ,SRC.DNF               
            ,SRC.DNS               
            ,SRC.DSQ               
            ,SRC.DRIVER_NUMBER     
            ,SRC.FULL_NAME         
            ,SRC.Q1_TIMING         
            ,SRC.Q2_TIMING         
            ,SRC.Q3_TIMING         
            ,SRC.Q1_GLT            
            ,SRC.Q2_GLT            
            ,SRC.Q3_GLT            
            ,SRC.POSITION          
            ,SRC.NUMBER_OF_LAPS
            );
