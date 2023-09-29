CREATE TABLE xdb_sys_current_process_executions(
    process_id VARCHAR(255) NOT NULL,
    process_execution_id BYTEA NOT NULL,
    PRIMARY KEY (process_id, process_execution_id)
);

CREATE TABLE xdb_sys_process_executions(
    id BYTEA NOT NULL,
    process_id VARCHAR(255) NOT NULL,
    --
    is_current BOOLEAN NOT NULL , -- duplicate info, but useful for debugging
    status VARCHAR(15) NOT NULL, -- running/timeout/completed/failed
    start_time TIMESTAMP NOT NULL,
    timeout_seconds INTEGER,
    history_event_id_sequence INTEGER, 
    info jsonb ,
    PRIMARY KEY (id)
);
