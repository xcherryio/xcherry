CREATE TABLE xcherry_sys_latest_process_executions(
    namespace VARCHAR(31) NOT NULL,
    process_id VARCHAR(255) NOT NULL,
    process_execution_id uuid NOT NULL,
    PRIMARY KEY (namespace, process_id)
);

CREATE TABLE xcherry_sys_process_executions(
    namespace VARCHAR(31) NOT NULL, -- for quick debugging
    id uuid NOT NULL,
    process_id VARCHAR(255) NOT NULL,
    --
    shard_id INTEGER,
    status SMALLINT, -- 0:undefined/1:running/2:completed/3:failed/4:timeout/5:terminated
    start_time TIMESTAMP NOT NULL,
    timeout_seconds INTEGER,
    history_event_id_sequence INTEGER,
    state_execution_sequence_maps jsonb NOT NULL , -- some maps from stateId and sequence number
    state_execution_local_queues jsonb, -- some maps to quickly consume received local queue messages
    graceful_complete_requested BOOLEAN NOT NULL DEFAULT false, -- if set to true, the process will be gracefully completed when there is no running state
    info jsonb , -- workerURL, processType, etc
    PRIMARY KEY (id)
);

CREATE TABLE xcherry_sys_async_state_executions(
   process_execution_id uuid NOT NULL,
   state_id VARCHAR(255) NOT NULL,
   state_id_sequence INTEGER NOT NULL,
   --
   version INTEGER NOT NULL , -- for conditional update to avoid locking
   status SMALLINT NOT NULL, -- 1:wait_until running/2:wait_until waiting/3:execute running/4:completed/5:failed/6:timeout/7:aborted
   wait_until_commands jsonb,
   wait_until_command_results jsonb,
   info jsonb ,
   input jsonb,
   last_failure jsonb,
   PRIMARY KEY (process_execution_id, state_id, state_id_sequence)
);

CREATE TABLE xcherry_sys_immediate_tasks(
    shard_id INTEGER NOT NULL, -- for virtual sharding
    task_sequence bigserial,   
    --
    task_type SMALLINT NOT NULL, -- 1: waitUntil 2: execute 3: localQueueMessage
    process_execution_id uuid,
    -- if the `task_type` is localQueueMessage, the value of state_id is "".
    state_id VARCHAR(255), -- for looking up xcherry_sys_async_state_executions
    -- if the `task_type` is localQueueMessage, the value of state_id_sequence is 0.
    state_id_sequence INTEGER, -- for looking up xcherry_sys_async_state_executions
    -- the info represents various information depending on the `task_type`:
    -- if the `task_type` is waitUntil or execute, the value corresponds to the state execution information.
    -- if the `task_type` is localQueueMessage, the value corresponds to the message information.
    info jsonb,
    PRIMARY KEY (shard_id, task_sequence)
);

CREATE TABLE xcherry_sys_timer_tasks(
    shard_id INTEGER NOT NULL, -- for virtual sharding
    fire_time_unix_seconds BIGINT NOT NULL, 
    task_sequence bigserial, -- to help ensure the PK uniqueness 
    --
    task_type SMALLINT, -- 1: process timeout 2: user timer command, 3: worker_task_backoff
    process_execution_id uuid, -- for looking up xcherry_sys_async_state_executions
    state_id VARCHAR(255), -- for looking up xcherry_sys_async_state_executions
    state_id_sequence INTEGER, -- for looking up xcherry_sys_async_state_executions
    info jsonb ,
    PRIMARY KEY (shard_id, fire_time_unix_seconds, task_sequence)    
);

CREATE TABLE xcherry_sys_local_queue_messages(
    process_execution_id uuid,
    dedup_id uuid,
    queue_name VARCHAR(31),
    payload jsonb,
    PRIMARY KEY (process_execution_id, dedup_id)
);

CREATE TABLE xcherry_sys_local_attributes(
    process_execution_id uuid NOT NULL,
    key VARCHAR(31) NOT NULL,
    value jsonb,
    PRIMARY KEY (process_execution_id, key)
);

CREATE TABLE xcherry_sys_executions_visibility (
    namespace VARCHAR(31) NOT NULL,
    process_id VARCHAR(255) NOT NULL,
    process_execution_id uuid NOT NULL,
    process_type_name VARCHAR(255) NOT NULL,
    status SMALLINT, -- 0:undefined/1:running/2:completed/3:failed/4:timeout/5:terminated
    start_time TIMESTAMP NOT NULL,
    close_time TIMESTAMP NULL,
    PRIMARY KEY (namespace, process_execution_id)
);

CREATE INDEX by_start_time ON xcherry_sys_executions_visibility (namespace, start_time DESC, process_execution_id);

CREATE INDEX by_type_start_time ON xcherry_sys_executions_visibility (namespace, process_type_name, start_time DESC, process_execution_id);

CREATE INDEX by_process_id_start_time ON xcherry_sys_executions_visibility (namespace, process_id, start_time DESC, process_execution_id);

CREATE INDEX by_status_start_time ON xcherry_sys_executions_visibility (namespace, status, start_time DESC, process_execution_id);

CREATE INDEX by_status_type_start_time ON xcherry_sys_executions_visibility (namespace, status, process_type_name, start_time DESC, process_execution_id);
