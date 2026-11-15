#!/usr/bin/env bash
set -euo pipefail

revision=4df2823bafface2ada35f482c07ff6668cc078e1
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
work=$(mktemp -d)
report_dir=${GINISTACK_REPORT_DIR:-"$root/.fidelity-reports"}
addr=${GINISTACK_TEST_ADDR:-127.0.0.1:14566}
go_binary=${GO:-go}
mode=${1:-}
if [[ $# -gt 1 || ($mode != "" && $mode != "--all-sqs") ]]; then
  echo "Usage: $0 [--all-sqs]" >&2
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
if [[ $mode == "--all-sqs" ]]; then
  workers=20
  if [[ -n ${GINISTACK_CASE_TIMEOUT:-} ]]; then
    timeout_args=(--timeout "$GINISTACK_CASE_TIMEOUT")
  fi
  checks=()
  for path in "$suite"/behavioral/sqs/check_*.py; do
    check=${path##*/check_}
    checks+=("${check%.py}")
  done
fi

run_check() {
  local check=$1
  PYTHONPATH="$suite/src" uv run --no-project --python 3.12 \
    --with 'boto3==1.43.108' --with 'botocore==1.43.108' --with 'certifi==2026.10.18' \
    python -m fidelity_tests_runner run "$suite/behavioral/sqs/check_$check.py" \
    --endpoint "http://$addr" --region us-west-2 --auth off --strict-cleanup "${timeout_args[@]}" \
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
