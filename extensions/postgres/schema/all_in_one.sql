CREATE TABLE xdb_sys_current_process_executions(
    namespace VARCHAR(15) NOT NULL,
    process_id VARCHAR(255) NOT NULL,
    process_execution_id BYTEA NOT NULL,
    PRIMARY KEY (namespace, process_id)
);

CREATE TABLE xdb_sys_process_executions(
    namespace VARCHAR(15) NOT NULL, -- for quick debugging
    id BYTEA NOT NULL,
    process_id VARCHAR(255) NOT NULL,
    --
    is_current BOOLEAN NOT NULL , -- for quick debugging
    status SMALLINT, -- 0:undefined/1:running/2:completed/3:failed/4:timeout
    start_time TIMESTAMP NOT NULL,
    timeout_seconds INTEGER,
    history_event_id_sequence INTEGER, 
    info jsonb , -- workerURL, processType, etc
    state_id_sequence jsonb, -- a map from stateId to the sequence number
    PRIMARY KEY (id)
);

CREATE TABLE xdb_sys_async_state_executions(
   process_execution_id BYTEA NOT NULL,
   state_id String NOT NULL,
   state_id_sequence INTEGER NOT NULL,
   --
   wait_until_status SMALLINT NOT NULL, -- -1: skipped/0:undefined/1:running/2:completed/3:failed/4:timeout
   execute_status SMALLINT, -- 0:undefined/1:running/2:completed/3:failed/4:timeout
   wait_until_commands jsonb,
   wait_until_command_results jsonb,
   info jsonb ,
   PRIMARY KEY (process_execution_id, state_id, state_id_sequence)
);

CREATE TABLE xdb_sys_worker_tasks(
    shard_id INTEGER NOT NULL, -- for sharding xdb-local-mq
    task_sequence bigserial,   
    --
    process_execution_id BYTEA NOT NULL, -- for looking up xdb_sys_async_state_executions
    state_id String NOT NULL, -- for looking up xdb_sys_async_state_executions
    state_id_sequence INTEGER NOT NULL, -- for looking up xdb_sys_async_state_executions
    info jsonb ,
    PRIMARY KEY (shard_id, task_sequence)
);

CREATE TABLE xdb_sys_timer_tasks(
    shard_id INTEGER NOT NULL, -- for sharding xdb-local-mq
    start_time TIMESTAMP NOT NULL,
    task_sequence bigserial, -- to help ensure the PK uniqueness 
    --
    process_execution_id BYTEA NOT NULL, -- for looking up xdb_sys_async_state_executions
    state_id String NOT NULL, -- for looking up xdb_sys_async_state_executions
    state_id_sequence INTEGER NOT NULL, -- for looking up xdb_sys_async_state_executions
    info jsonb ,
    PRIMARY KEY (shard_id, start_time, task_sequence)    
);