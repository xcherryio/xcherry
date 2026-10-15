// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

#[path = "integ/support.rs"]
mod support;

#[path = "integ/any_command_combination_test.rs"]
mod any_command_combination_test;
#[path = "integ/any_command_combination_workflow.rs"]
mod any_command_combination_workflow;
#[path = "integ/basic_abnormal_exit_workflow.rs"]
mod basic_abnormal_exit_workflow;
#[path = "integ/basic_empty_input_workflow.rs"]
mod basic_empty_input_workflow;
#[path = "integ/basic_immutable_step_options_workflow.rs"]
mod basic_immutable_step_options_workflow;
#[path = "integ/basic_model_input_workflow.rs"]
mod basic_model_input_workflow;
#[path = "integ/basic_proceed_on_wait_failure_workflow.rs"]
mod basic_proceed_on_wait_failure_workflow;
#[path = "integ/basic_test.rs"]
mod basic_test;
#[path = "integ/basic_workflow.rs"]
mod basic_workflow;
#[path = "integ/conditional_complete_test.rs"]
mod conditional_complete_test;
#[path = "integ/conditional_complete_workflow.rs"]
mod conditional_complete_workflow;
#[path = "integ/flow_service_client.rs"]
mod flow_service_client;
#[path = "integ/heartbeat_recovery_test.rs"]
mod heartbeat_recovery_test;
#[path = "integ/heartbeat_recovery_workflow.rs"]
mod heartbeat_recovery_workflow;
#[path = "integ/internal_channel_test.rs"]
mod internal_channel_test;
#[path = "integ/internal_channel_waiting_workflow.rs"]
mod internal_channel_waiting_workflow;
#[path = "integ/internal_channel_workflow.rs"]
mod internal_channel_workflow;
#[path = "integ/multi_output_test.rs"]
mod multi_output_test;
#[path = "integ/multi_output_workflow.rs"]
mod multi_output_workflow;
#[path = "integ/no_start_state_dead_end_workflow.rs"]
mod no_start_state_dead_end_workflow;
#[path = "integ/no_start_state_test.rs"]
mod no_start_state_test;
#[path = "integ/no_start_state_workflow.rs"]
mod no_start_state_workflow;
#[path = "integ/persistence_set_attributes_workflow.rs"]
mod persistence_set_attributes_workflow;
#[path = "integ/persistence_test.rs"]
mod persistence_test;
#[path = "integ/persistence_workflow.rs"]
mod persistence_workflow;
#[path = "integ/pull_worker_test.rs"]
mod pull_worker_test;
#[path = "integ/reset_test.rs"]
mod reset_test;
#[path = "integ/reset_workflow.rs"]
mod reset_workflow;
#[path = "integ/rpc_no_state_workflow.rs"]
mod rpc_no_state_workflow;
#[path = "integ/rpc_test.rs"]
mod rpc_test;
#[path = "integ/rpc_with_memo_test.rs"]
mod rpc_with_memo_test;
#[path = "integ/rpc_workflow.rs"]
mod rpc_workflow;
#[path = "integ/search_flows_test.rs"]
mod search_flows_test;
#[path = "integ/search_flows_workflow.rs"]
mod search_flows_workflow;
#[path = "integ/signal_test.rs"]
mod signal_test;
#[path = "integ/signal_workflow.rs"]
mod signal_workflow;
#[path = "integ/skip_wait_until_mixed_wait_workflow.rs"]
mod skip_wait_until_mixed_wait_workflow;
#[path = "integ/skip_wait_until_test.rs"]
mod skip_wait_until_test;
#[path = "integ/skip_wait_until_workflow.rs"]
mod skip_wait_until_workflow;
#[path = "integ/state_options_locking_workflow.rs"]
mod state_options_locking_workflow;
#[path = "integ/state_options_override_test.rs"]
mod state_options_override_test;
#[path = "integ/state_options_override_workflow.rs"]
mod state_options_override_workflow;
#[path = "integ/state_options_test.rs"]
mod state_options_test;
#[path = "integ/state_options_workflow.rs"]
mod state_options_workflow;
#[path = "integ/state_recovery_no_wait_workflow.rs"]
mod state_recovery_no_wait_workflow;
#[path = "integ/state_recovery_test.rs"]
mod state_recovery_test;
#[path = "integ/state_recovery_workflow.rs"]
mod state_recovery_workflow;
#[path = "integ/step_cancellation_test.rs"]
mod step_cancellation_test;
#[path = "integ/step_cancellation_workflow.rs"]
mod step_cancellation_workflow;
#[path = "integ/stream_test.rs"]
mod stream_test;
#[path = "integ/stream_workflow.rs"]
mod stream_workflow;
#[path = "integ/subflow_test.rs"]
mod subflow_test;
#[path = "integ/subflow_workflow.rs"]
mod subflow_workflow;
#[path = "integ/timer_test.rs"]
mod timer_test;
#[path = "integ/timer_workflow.rs"]
mod timer_workflow;
#[path = "integ/worker_origin_stack_test.rs"]
mod worker_origin_stack_test;
#[path = "integ/worker_origin_stack_workflow.rs"]
mod worker_origin_stack_workflow;
#[path = "integ/worker_retry_after_test.rs"]
mod worker_retry_after_test;
#[path = "integ/worker_retry_after_workflow.rs"]
mod worker_retry_after_workflow;
#[path = "integ/workflow_uncompleted_empty_decision_workflow.rs"]
mod workflow_uncompleted_empty_decision_workflow;
#[path = "integ/workflow_uncompleted_force_fail_workflow.rs"]
mod workflow_uncompleted_force_fail_workflow;
#[path = "integ/workflow_uncompleted_state_failure_workflow.rs"]
mod workflow_uncompleted_state_failure_workflow;
#[path = "integ/workflow_uncompleted_state_timeout_workflow.rs"]
mod workflow_uncompleted_state_timeout_workflow;
#[path = "integ/workflow_uncompleted_test.rs"]
mod workflow_uncompleted_test;
