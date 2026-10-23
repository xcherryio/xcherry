CREATE EXTENSION IF NOT EXISTS pg_search;

CREATE TABLE dexzd_visibility_runs (
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    workflow_id text NOT NULL,
    workflow_type text NOT NULL,
    flow_type text NOT NULL,
    execution_status text NOT NULL,
    is_running boolean GENERATED ALWAYS AS (execution_status = 'Running') STORED,
    start_time_ns bigint NOT NULL,
    close_time_ns bigint,
    dex_parent_flow_id text,
    active_step_types text[],
    dex_work_queue_permissions text[],
    version bigint NOT NULL,
    sa_keyword jsonb NOT NULL DEFAULT '{}'::jsonb,
    sa_text jsonb NOT NULL DEFAULT '{}'::jsonb,
    sa_number jsonb NOT NULL DEFAULT '{}'::jsonb,
    sa_datetime jsonb NOT NULL DEFAULT '{}'::jsonb,
    sa_bool jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (tenant_id, run_id)
);

CREATE INDEX dexzd_visibility_default_idx ON dexzd_visibility_runs
    (tenant_id, is_running DESC, close_time_ns DESC NULLS LAST, start_time_ns DESC, run_id COLLATE "C");
CREATE INDEX dexzd_visibility_flow_type_idx ON dexzd_visibility_runs
    (tenant_id, flow_type, is_running DESC, close_time_ns DESC NULLS LAST, start_time_ns DESC, run_id COLLATE "C");
CREATE INDEX dexzd_visibility_workflow_id_idx ON dexzd_visibility_runs
    (tenant_id, workflow_id);
CREATE INDEX dexzd_visibility_bm25_idx ON dexzd_visibility_runs USING paradedb (
    (tenant_id::pdb.literal),
    (run_id::pdb.literal),
    (workflow_id::pdb.literal),
    (workflow_type::pdb.literal),
    (flow_type::pdb.literal),
    (execution_status::pdb.literal),
    (dex_parent_flow_id::pdb.literal),
    start_time_ns, close_time_ns,
    (sa_keyword::pdb.literal),
    (sa_text::pdb.whitespace),
    (sa_number::pdb.whitespace('columnar=true')),
    (sa_datetime::pdb.whitespace('columnar=true')),
    (sa_bool::pdb.whitespace('columnar=true'))
);
