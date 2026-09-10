#!/usr/bin/env bash

# =============================================================================
# analyze_pcaps.sh
#
# Batch-ingests PCAPdroid captures through the network analyzer subsystem.
#
# For every capture in the inbox it runs `python -m network_analyzer.main`, then
# files the capture according to the analyzer's exit code:
#
#   success            -> moved to the processed directory
#   retryable failure  -> left in the inbox, so the next run picks it up again
#   rejected input     -> moved to the failed directory, with a .log beside it
#
# That routing is the whole point of the script: an unreachable broker must not
# quarantine a perfectly good capture, and a malformed capture must not be
# retried forever.
#
# Captures are expected to be named
#   <package_name>__<upload|download>__<YYYYMMDDTHHMMSS>.pcap
# for example
#   com.whatsapp__upload__20260907T141500.pcap
# because that is how the analyzer learns which application and scenario a file
# belongs to without per-file arguments.
#
# Usage:
#   ./scripts/network/analyze_pcaps.sh [options]
#   ./scripts/network/analyze_pcaps.sh --help
#
# Prerequisites: the analyzer's dependencies installed in a Python environment
# (see network_analyzer/requirements.txt), plus Kafka and the App API reachable
# unless --dry-run and --skip-registry-check are used.
# =============================================================================

# Note: intentionally not `set -e`. The loop has to survive a failing capture so
# it can report a summary across the whole batch, matching check_infra.sh.
set -uo pipefail

# --- Appearance --------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Exit codes reported by network_analyzer.main ----------------------------
readonly ANALYZER_EXIT_OK=0
readonly ANALYZER_EXIT_TRANSPORT_FAILURE=5

# --- Paths -------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- Defaults ----------------------------------------------------------------
INBOX_DIR="$PROJECT_ROOT/data/pcap/inbox"
PROCESSED_DIR="$PROJECT_ROOT/data/pcap/processed"
FAILED_DIR="$PROJECT_ROOT/data/pcap/failed"
PYTHON_BIN="${ANALYZER_PYTHON:-}"
DRY_RUN=false
SKIP_REGISTRY_CHECK=false
KEEP_FILES=false
VERBOSE=false

# --- Counters ----------------------------------------------------------------
ANALYZED=0
FAILED=0
RETRYABLE=0

# --- Helpers -----------------------------------------------------------------

print_header() {
    echo ""
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_ok() {
    echo -e "  ${GREEN}[OK]${NC} $1"
}

print_fail() {
    echo -e "  ${RED}[FAIL]${NC} $1"
}

print_warn() {
    echo -e "  ${YELLOW}[WARN]${NC} $1"
}

print_info() {
    echo -e "  ${YELLOW}[..]${NC} $1"
}

usage() {
    cat <<'EOF'
Batch-analyze PCAPdroid captures and publish their metrics to Kafka.

Usage:
  ./scripts/network/analyze_pcaps.sh [options]

Options:
  --inbox DIR             Directory holding captures to analyze.
                          Default: data/pcap/inbox
  --processed DIR         Where successfully analyzed captures are moved.
                          Default: data/pcap/processed
  --failed DIR            Where rejected captures are moved, with a .log file.
                          Default: data/pcap/failed
  --python PATH           Python interpreter to use. Defaults to $ANALYZER_PYTHON,
                          then an auto-detected project virtualenv, then python3.
  --dry-run               Compute and print metrics without publishing to Kafka.
  --skip-registry-check   Do not verify package names against the App API.
  --keep                  Analyze in place; do not move any capture files.
  --verbose               Pass --verbose to the analyzer for debug logging.
  -h, --help              Show this help and exit.

Exit status:
  0  every capture was analyzed (or the inbox was empty)
  1  at least one capture failed or still needs a retry

Examples:
  # Normal run: analyze everything new and publish it.
  ./scripts/network/analyze_pcaps.sh

  # Inspect the numbers first, touching neither Kafka nor the App API.
  ./scripts/network/analyze_pcaps.sh --dry-run --skip-registry-check --keep

  # Analyze captures sitting somewhere else.
  ./scripts/network/analyze_pcaps.sh --inbox ~/Downloads/PCAPdroid
EOF
}

# Resolve the interpreter: explicit flag, then env var, then a project
# virtualenv, then whatever python3 is on PATH.
resolve_python() {
    if [[ -n "$PYTHON_BIN" ]]; then
        return 0
    fi

    local candidate
    for candidate in \
        "$PROJECT_ROOT/.venv-analyzer/bin/python" \
        "$PROJECT_ROOT/.venv/bin/python" \
        "$PROJECT_ROOT/venv/bin/python"; do
        if [[ -x "$candidate" ]]; then
            PYTHON_BIN="$candidate"
            return 0
        fi
    done

    PYTHON_BIN="$(command -v python3 || true)"
}

# Fail early with actionable advice rather than letting every capture die on the
# same ImportError.
check_python() {
    if [[ -z "$PYTHON_BIN" ]] || [[ ! -x "$PYTHON_BIN" ]]; then
        if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
            echo -e "${RED}No usable Python interpreter found.${NC}" >&2
            echo "Pass one with --python /path/to/python." >&2
            exit 1
        fi
    fi

    if ! "$PYTHON_BIN" -c 'import dpkt, kafka, requests, dotenv' >/dev/null 2>&1; then
        echo -e "${RED}$PYTHON_BIN is missing the analyzer's dependencies.${NC}" >&2
        echo "Install them with:" >&2
        echo "  $PYTHON_BIN -m pip install -r network_analyzer/requirements.txt" >&2
        exit 1
    fi
}

# --- Argument parsing --------------------------------------------------------
# Hand-rolled rather than getopts, because bash's getopts does not support the
# long options this script needs to stay readable in documentation.

while [[ $# -gt 0 ]]; do
    case "$1" in
        --inbox)
            [[ $# -ge 2 ]] || { echo "--inbox needs a directory." >&2; exit 2; }
            INBOX_DIR="$2"
            shift 2
            ;;
        --processed)
            [[ $# -ge 2 ]] || { echo "--processed needs a directory." >&2; exit 2; }
            PROCESSED_DIR="$2"
            shift 2
            ;;
        --failed)
            [[ $# -ge 2 ]] || { echo "--failed needs a directory." >&2; exit 2; }
            FAILED_DIR="$2"
            shift 2
            ;;
        --python)
            [[ $# -ge 2 ]] || { echo "--python needs a path." >&2; exit 2; }
            PYTHON_BIN="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-registry-check)
            SKIP_REGISTRY_CHECK=true
            shift
            ;;
        --keep)
            KEEP_FILES=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Run with --help to see the available options." >&2
            exit 2
            ;;
    esac
done

# --- Preflight ---------------------------------------------------------------

echo -e "${BLUE}"
echo "+----------------------------------------------+"
echo "|      PCAPdroid capture batch analysis        |"
echo "+----------------------------------------------+"
echo -e "${NC}"

resolve_python
check_python

if [[ ! -d "$INBOX_DIR" ]]; then
    echo -e "${RED}Inbox directory not found: $INBOX_DIR${NC}" >&2
    echo "Create it and drop your captures in, or pass --inbox DIR." >&2
    exit 1
fi

print_header "Configuration"
print_info "Interpreter  : $PYTHON_BIN"
print_info "Inbox        : $INBOX_DIR"
if [[ "$KEEP_FILES" == true ]]; then
    print_info "File moves   : disabled (--keep)"
else
    print_info "Processed    : $PROCESSED_DIR"
    print_info "Failed       : $FAILED_DIR"
fi
if [[ "$DRY_RUN" == true ]]; then
    print_info "Mode         : dry run (nothing will be published)"
else
    print_info "Mode         : publishing to the network-metrics topic"
fi

# Collect captures. nullglob keeps an unmatched pattern from becoming a literal
# argument, and the array preserves filenames containing spaces.
shopt -s nullglob
CAPTURES=("$INBOX_DIR"/*.pcap "$INBOX_DIR"/*.pcapng)
shopt -u nullglob

if [[ ${#CAPTURES[@]} -eq 0 ]]; then
    print_header "Result"
    print_ok "No captures waiting in $INBOX_DIR. Nothing to do."
    exit 0
fi

if [[ "$KEEP_FILES" != true ]]; then
    mkdir -p "$PROCESSED_DIR" "$FAILED_DIR" || {
        echo -e "${RED}Could not create the output directories.${NC}" >&2
        exit 1
    }
fi

# --- Build the shared analyzer arguments -------------------------------------

ANALYZER_ARGS=()
[[ "$DRY_RUN" == true ]] && ANALYZER_ARGS+=(--dry-run)
[[ "$SKIP_REGISTRY_CHECK" == true ]] && ANALYZER_ARGS+=(--skip-registry-check)
[[ "$VERBOSE" == true ]] && ANALYZER_ARGS+=(--verbose)

# --- Analyze -----------------------------------------------------------------

print_header "Analyzing ${#CAPTURES[@]} capture(s)"

for capture in "${CAPTURES[@]}"; do
    name="$(basename "$capture")"
    echo ""
    print_info "$name"

    # Assign first, then read $?. Wrapping this in `if ! output=$(...)` would
    # make $? the status of the negation (always 0), not the analyzer's exit
    # code, and every failure would be filed as a success.
    output=$(
        cd "$PROJECT_ROOT" && \
        "$PYTHON_BIN" -m network_analyzer.main \
            --file "$capture" \
            "${ANALYZER_ARGS[@]}" 2>&1
    )
    status=$?

    if [[ -n "$output" ]]; then
        printf '%s\n' "$output" | sed 's/^/      /'
    fi

    if [[ $status -eq $ANALYZER_EXIT_OK ]]; then
        ANALYZED=$((ANALYZED + 1))
        if [[ "$KEEP_FILES" == true ]]; then
            print_ok "$name analyzed (left in place)."
        elif mv -- "$capture" "$PROCESSED_DIR/$name"; then
            print_ok "$name analyzed, moved to processed/."
        else
            print_warn "$name analyzed, but could not be moved out of the inbox."
        fi
        continue
    fi

    if [[ $status -eq $ANALYZER_EXIT_TRANSPORT_FAILURE ]]; then
        RETRYABLE=$((RETRYABLE + 1))
        print_warn "$name could not reach a required service; left in the inbox for a retry."
        continue
    fi

    FAILED=$((FAILED + 1))
    if [[ "$KEEP_FILES" == true ]]; then
        print_fail "$name was rejected (exit $status); left in place."
        continue
    fi

    if mv -- "$capture" "$FAILED_DIR/$name"; then
        printf '%s\n' "$output" > "$FAILED_DIR/$name.log"
        print_fail "$name was rejected (exit $status); moved to failed/ with a .log."
    else
        print_fail "$name was rejected (exit $status), and could not be moved."
    fi
done

# --- Summary -----------------------------------------------------------------

print_header "Result"
echo -e "  Analyzed        : ${GREEN}$ANALYZED${NC}"
echo -e "  Needs a retry   : ${YELLOW}$RETRYABLE${NC}"
echo -e "  Rejected        : ${RED}$FAILED${NC}"
echo -e "  Total inspected : ${#CAPTURES[@]}"

if [[ $FAILED -eq 0 && $RETRYABLE -eq 0 ]]; then
    echo ""
    print_ok "All captures analyzed successfully."
    if [[ "$DRY_RUN" != true ]]; then
        echo "  Confirm the messages landed with:"
        echo "    ./scripts/verify/verify_network_metrics.sh"
    fi
    exit 0
fi

echo ""
if [[ $RETRYABLE -gt 0 ]]; then
    print_warn "$RETRYABLE capture(s) still in the inbox. Check that Kafka and the App API are up, then run this script again."
fi
if [[ $FAILED -gt 0 ]]; then
    print_fail "$FAILED capture(s) were rejected. Read the .log files in $FAILED_DIR for the reason."
fi
exit 1
