#!/usr/bin/env bash
set -euo pipefail

revision=4df2823bafface2ada35f482c07ff6668cc078e1
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
work=$(mktemp -d)
report_dir=${GINISTACK_REPORT_DIR:-"$root/.fidelity-reports"}
addr=${GINISTACK_TEST_ADDR:-127.0.0.1:14566}
go_binary=${GO:-go}
mode=${1:-}
if [[ $# -gt 1 || ($mode != "" && $mode != "--all-sqs" && $mode != "--sns" && $mode != "--all-sns") ]]; then
  echo "Usage: $0 [--all-sqs | --sns | --all-sns]" >&2
  exit 2
fi
pid=
trap 'if [[ -n $pid ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; rm -rf -- "$work"' EXIT
mkdir -p "$report_dir"

suite=${AWS_SUITE_DIR:-"$work/aws-suite"}
if [[ ! -v AWS_SUITE_DIR ]]; then
  git init -q "$suite"
  git -C "$suite" fetch -q --depth 1 https://github.com/aws/aws-emulator-evaluation-suite.git "$revision"
  git -C "$suite" checkout -q --detach FETCH_HEAD
fi
suite=$(cd "$suite" && pwd)
if [[ $(git -C "$suite" rev-parse HEAD) != "$revision" || -n $(git -C "$suite" status --porcelain --untracked-files=no) ]]; then
  echo "Expected AWS suite revision $revision with no tracked edits" >&2
  exit 1
fi

(cd "$root" && "$go_binary" build -buildvcs=false -race -o "$work/ginistack" ./cmd/ginistack)
"$work/ginistack" -addr "$addr" >"$report_dir/server.log" 2>&1 &
pid=$!
for ((attempt=0; attempt<100; attempt++)); do
  if ! kill -0 "$pid" 2>/dev/null; then
    cat "$report_dir/server.log" >&2
    exit 1
  fi
  if curl -fsS --max-time 1 "http://$addr/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep .1
done
curl -fsS --max-time 1 "http://$addr/healthz" >/dev/null

checks=(
  max_message_size
  default_max_message_size_is_one_mib_boundary
  sse_sqs_managed_encryption_round_trips_body_byte_exact
  add_remove_permission
  add_remove_permission_updates_policy
  add_permission_invalid_action_rejected
  add_permission_over_limit_rejected
  remove_permission_nonexistent_label_rejected
  op_tag_and_permission_validation_rejected
  create_queue_existing_name_different_policy_not_conflicting
  queue_policy_attribute_normalizes_principal_and_clears
  set_queue_attributes_invalid_policy_json_rejected
  queue_policy_explicit_deny_blocks_send
  send_message_malformed_awstraceheader_accepted_then_dropped
  sender_id_and_sent_timestamp_derive_from_the_caller_and_the_live_clock
  send_receive_delete_lifecycle
  receive_message_visibility_timeout_override_per_call
  change_message_visibility_zero_makes_immediately_visible
  delete_message_stale_receipt_handle_after_redelivery_succeeds
  op_receive_message_param_bounds_rejected
  receive_message_respects_max_number_of_messages_cap
  long_poll_wakes_when_delay_or_visibility_expires
  receive_count_and_first_receive_timestamp_invariants_across_redeliveries
  random_body_roundtrip
  create_queue_existing_name_different_tags_rejected
  send_message_batch_partial_failure
  batch_send_partial_mixed
  batch_partial_failure_envelope_fields
  send_message_batch_duplicate_ids_rejected_atomically
  delete_and_visibility_batches_duplicate_ids_rejected
  delete_and_visibility_batches_over_ten_entries_rejected
  delete_message_batch_empty_request
  delete_message_batch_partial_failure
  change_message_visibility_batch_partial
  change_message_visibility_batch_repeated_receipt_handle_last_entry_wins
  sqsbatch_delete_duplicate_receipt_handle_deleted_once
  batch_entry_id_character_and_length_rules_rejected
  batch_total_size_exceeds_limit_rejected
  fifo_ordering
  fifo_content_based_dedup_ignores_attributes
  fifo_dedup_survives_consumer_delete
  fifo_receive_request_attempt_id_replays_batch
  fifo_group_blocked_by_a_poison_message_resumes_after_the_dlq_move
  dead_lettered_message_keeps_its_identity_and_names_its_source_queue
  message_attributes_round_trip_with_md5
  receive_message_attribute_name_wildcard_selects_by_name_prefix
  list_queues_max_results_paginates_with_next_token
  list_queues_pagination_bad_input_rejected
  purge_queue_in_progress_rejected
  tag_queue_over_fifty_tags_rejected
  maximum_message_size_is_a_byte_boundary_not_a_character_count
  queue_delay_change_is_retroactive_for_fifo_but_not_standard
  query_and_json_protocol_clients_share_one_queue
  query_protocol_refusals_are_xml_error_responses_rejected
  list_dead_letter_source_queues_paginates
  redrive_allow_policy_document_validation_rejected
  message_move_task_redrives_dlq_back_to_source
  message_attribute_type_and_number_value_validated
  receive_request_attempt_id_not_retryable_after_batch_modified_rejected
  same_name_queues_in_two_regions_hold_separate_messages
)

workers=6
timeout_args=()
service=sqs
if [[ $mode == "--all-sqs" || $mode == "--all-sns" ]]; then
  workers=20
  if [[ -n ${GINISTACK_CASE_TIMEOUT:-} ]]; then
    timeout_args=(--timeout "$GINISTACK_CASE_TIMEOUT")
  fi
  directories=(sqs)
  if [[ $mode == "--all-sns" ]]; then
    directories=(sns__sqs sns)
  fi
  checks=()
  for directory in "${directories[@]}"; do
    for path in "$suite/behavioral/$directory"/check_*.py; do
      check=${path##*/check_}
      checks+=("$directory/${check%.py}")
    done
  done
elif [[ $mode == "--sns" ]]; then
  workers=12
  service=sns
  checks=(
    sns__sqs/a_message_filtered_out_at_publish_time_is_never_redelivered_when_the_policy_is_relaxed
    clearing_a_filter_policy_with_an_empty_value_restores_unfiltered_delivery
    concurrent_create_topic_same_name_converges
    concurrent_tag_resource_distinct_keys_all_retained
    create_topic_idempotent_and_conflict
    create_topic_invalid_attribute_creates_nothing
    create_topic_with_different_tags_rejected
    delete_topic_cascades_subscriptions
    display_name_over_100_characters_rejected
    empty_vs_absent_message_attribute_rejected
    fifo_message_group_id_validation
    fifo_throughput_scope_message_group_narrows_the_deduplication_scope
    fifo_throughput_scope_on_a_standard_topic_or_an_unknown_value_rejected
    fifo_topic_attributes_roundtrip
    fifo_topic_ordering_delivery
    get_topic_attributes_malformed_arn_rejected
    list_subscriptions_by_topic_pagination_exactness
    list_subscriptions_paginates_and_drops_unsubscribed
    list_tags_for_nonexistent_topic_fails
    list_topics_and_subscriptions_forged_next_token_rejected
    list_topics_paginates_every_created_topic_exactly_once
    message_structure_json_selects_the_protocol_body_and_falls_back_to_default
    notification_signature_verifies_against_the_signing_certificate
    op_create_topic_invalid_name_rejected
    publish_by_target_arn_naming_a_topic_delivers_like_topic_arn
    publish_of_exactly_256_kib_delivered_intact_and_one_byte_more_rejected
    publish_size_limit_counts_message_attributes
    publish_still_delivers_to_siblings_when_a_subscribed_queue_is_deleted
    publish_with_a_reserved_attribute_name_prefix_rejected
    raw_delivery_to_sqs_drops_a_message_with_more_than_ten_attributes
    raw_message_delivery_flag_is_case_insensitive_and_reads_back_lowercase
    set_topic_attributes_invalid_signature_version_rejected
    set_topic_attributes_invalid_tracing_config_rejected
    set_topic_attributes_unknown_name_fails
    sqs_subscription_delivers_the_published_message_unconditionally
    standard_topic_rejects_fifo_dedup_id
    subject_with_a_line_break_or_control_character_rejected
    subscribe_invalid_endpoint_rejected
    subscribe_same_endpoint_is_idempotent
    tag_resource_lifecycle
    tag_resource_lifecycle_and_limit
    topic_set_attribute_immutable_rejected
    unsubscribe_malformed_arn_fails
    unsubscribe_nonexistent_is_idempotent
    unsubscribe_stops_delivery
    unsubscribe_stops_delivery_and_attribute_lookup_is_rejected
    sns__sqs/fifo_topic_subscription_filter_policy_withholds_non_matching_messages
    sns__sqs/queue_maximum_message_size_does_not_bound_sns_delivery
    sns__sqs/sns_sqs_queue_policy_deny_suppresses_delivery
    sns__sqs/sns_to_sqs_envelope_carries_the_subject_and_typed_message_attributes
    sns__sqs/sqs_envelope_raw_bytes_are_pretty_printed_in_canonical_key_order
  )
fi

run_check() {
  local check=$1 directory=$service
  local -a case_timeout=("${timeout_args[@]}")
  if [[ $check == */* ]]; then
    directory=${check%/*}
    check=${check##*/}
  fi
  local -a dependencies=()
  if [[ $directory == sns ]]; then
    dependencies=(--with 'requests==2.34.2')
  fi
  if [[ $check == a_message_filtered_out_at_publish_time_is_never_redelivered_when_the_policy_is_relaxed ]]; then
    # Long polls and two deliberate absence windows can exceed the runner's 60-second default.
    case_timeout=(--timeout "${GINISTACK_CASE_TIMEOUT:-120}")
  fi
  PYTHONPATH="$suite/src" uv run --no-project --python 3.12 \
    --with 'boto3==1.43.108' --with 'botocore==1.43.108' --with 'certifi==2026.10.18' \
    "${dependencies[@]}" \
    python -m fidelity_tests_runner run "$suite/behavioral/$directory/check_$check.py" \
    --endpoint "http://$addr" --region us-west-2 --auth off --strict-cleanup "${case_timeout[@]}" \
    --report "$report_dir/$check.json"
}

run_worker() {
  local index failures=0
  for ((index=$1; index<${#checks[@]}; index+=workers)); do
    if ! run_check "${checks[index]}"; then
      failures=$((failures+1))
    fi
  done
  return "$failures"
}

running=()
for ((worker=0; worker<workers; worker++)); do
  run_worker "$worker" &
  running+=("$!")
done
failed=0
for worker in "${running[@]}"; do
  status=0
  wait "$worker" || status=$?
  failed=$((failed+status))
done

if grep -q 'WARNING: DATA RACE' "$report_dir/server.log"; then
  cat "$report_dir/server.log" >&2
  exit 1
fi
printf '%d/%d casos oficiales aprobados\n' "$((${#checks[@]}-failed))" "${#checks[@]}"
((failed == 0))
