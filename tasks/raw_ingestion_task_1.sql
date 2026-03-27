create or replace task F1_ANALYTICS.RAW.LOAD_RESULTS_TASK
	warehouse=COMPUTE_WH
	schedule='USING CRON 30 22 * * SUN UTC'
	as begin
        copy into f1_analytics.raw.results
        from
        @F1_ANALYTICS_DATA_STAGE/Data/Results/
        file_format = (format_name = 'FF_JSON');

        merge into f1_analytics.raw.results tgt
        using(
            select RAW_LOGS from f1_analytics.raw.results
        ) src 
        on src.RAW_LOGS: "session_key" = tgt.RAW_LOGS: "session_key"
        when not matched then 
            insert (raw_logs) values(src.raw_logs);
        
    end;
