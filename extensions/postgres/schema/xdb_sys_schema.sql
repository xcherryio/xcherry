CREATE TABLE xdb_sys_current_process_executions(
    namespace VARCHAR(15) NOT NULL,
    process_id VARCHAR(255) NOT NULL,
    process_execution_id uuid NOT NULL,
    PRIMARY KEY (namespace, process_id)
);

CREATE TABLE xdb_sys_process_executions(
    namespace VARCHAR(15) NOT NULL, -- for quick debugging
    id uuid NOT NULL,
    process_id VARCHAR(255) NOT NULL,
    --
    is_current BOOLEAN NOT NULL , -- for quick debugging
    status SMALLINT, -- 0:undefined/1:running/2:completed/3:failed/4:timeout/5:terminated
    start_time TIMESTAMP NOT NULL,
    timeout_seconds INTEGER,
    history_event_id_sequence INTEGER,
    state_execution_sequence_maps jsonb NOT NULL , -- some maps from stateId and sequence number
    info jsonb , -- workerURL, processType, etc
    PRIMARY KEY (id)
);

CREATE TABLE xdb_sys_async_state_executions(
   process_execution_id uuid NOT NULL,
   state_id VARCHAR(32) NOT NULL,
   state_id_sequence INTEGER NOT NULL,
   --
   version INTEGER NOT NULL , -- for conditional update to avoid locking
   wait_until_status SMALLINT NOT NULL, -- -1: skipped/0:undefined/1:running/2:completed/3:failed/4:timeout
   execute_status SMALLINT, -- 0:undefined/1:running/2:completed/3:failed/4:timeout
   wait_until_commands jsonb,
   wait_until_command_results jsonb,
   info jsonb ,
   input jsonb,
   PRIMARY KEY (process_execution_id, state_id, state_id_sequence)
);

CREATE TABLE xdb_sys_worker_tasks(
    shard_id INTEGER NOT NULL, -- for sharding xdb-local-mq
    task_sequence bigserial,   
    --
    task_type SMALLINT NOT NULL, -- 1: waitUntil 2: execute
    process_execution_id uuid, -- for looking up xdb_sys_async_state_executions
    state_id VARCHAR(32), -- for looking up xdb_sys_async_state_executions
    state_id_sequence INTEGER, -- for looking up xdb_sys_async_state_executions
    info jsonb ,
    PRIMARY KEY (shard_id, task_sequence)
);

CREATE TABLE xdb_sys_timer_tasks(
    shard_id INTEGER NOT NULL, -- for sharding xdb-local-mq
    fire_time TIMESTAMP NOT NULL,
    task_sequence bigserial, -- to help ensure the PK uniqueness 
    --
    task_type SMALLINT, -- 1: process timeout 2: user timer command, 3: worker_task_backoff
    process_execution_id uuid, -- for looking up xdb_sys_async_state_executions
    state_id VARCHAR(32), -- for looking up xdb_sys_async_state_executions
    state_id_sequence INTEGER, -- for looking up xdb_sys_async_state_executions
    info jsonb ,
    PRIMARY KEY (shard_id, fire_time, task_sequence)    
);