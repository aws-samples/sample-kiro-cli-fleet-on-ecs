#!/usr/bin/env bash
#
# Refuses to let this repository carry anything that must not be published.
#
# Scans only the files git would actually commit, so ignored build output and
# virtual environments are out of scope by construction.
#
# Checks here are generic on purpose. Project-specific terms and the fingerprints
# of real credentials live in an untracked local file, because a denylist naming
# internal systems would itself disclose them once this repository is public.
#
#   ./tools/scan-secrets.sh          scan
#   ./tools/scan-secrets.sh --list   show what is being checked
#
# Local additions: see .secret-scan.local.example

set -euo pipefail

cd "$(dirname "$0")/.."

LOCAL_RULES=".secret-scan.local"
FINDINGS=0

# Prefix every line of a multi-line report with six spaces.
indent() { printf '      %s' "${1//$'\n'/$'\n'      }"; }

# Files excluded from content scanning: third-party text and machine-generated
# lockfiles produce false positives on digit-run patterns and carry no authored
# content that could leak.
is_excluded() {
    case "$1" in
        LICENSE | *.lock | *.png | *.jpg | *.jpeg | *.gif | *.pdf | *.svg) return 0 ;;
        tools/scan-secrets.sh) return 0 ;;  # contains the patterns themselves
        *) return 1 ;;
    esac
}

# --- generic rules -----------------------------------------------------------
# Each entry is "description|extended-regex".

RULES=(
    "AWS account number|(^|[^0-9])[0-9]{12}([^0-9]|$)"
    "ARN containing an account number|arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:[0-9]{12}"
    "AWS access key id|(AKIA|ASIA)[0-9A-Z]{16}"
    "private key block|-----BEGIN [A-Z ]*PRIVATE KEY-----"
    "VPC resource id|\b(vpc|subnet|sg|eni|igw|nat|rtb|acl|vpce)-[0-9a-f]{8,}"
    "EC2 instance id|\bi-[0-9a-f]{8,}\b"
    "absolute home directory path|/Users/[A-Za-z0-9._-]+/"
    "internal account-tool name|(^|[^A-Za-z0-9.])[Ii]sengard([^A-Za-z0-9.]|$)"
    "AWS SSO or Isengard host|[a-z0-9.-]*\.(awsapps|isengard)\.[a-z.]+"
)

# The canonical AWS documentation placeholder. Permitted anywhere, including test
# fixtures, because a well-formed ARN needs an account field and this value belongs
# to no one. Anything else matching the account-number shape is a finding.
ALLOWED_ACCOUNT_PLACEHOLDER="123456789012"

# --- local rules -------------------------------------------------------------

LOCAL_PATTERNS=()
FINGERPRINTS=()

load_local_rules() {
    [ -f "$LOCAL_RULES" ] || return 0
    while IFS= read -r line || [ -n "$line" ]; do
        case "$line" in
            '' | '#'*) continue ;;
            pattern:*) LOCAL_PATTERNS+=("${line#pattern:}") ;;
            fingerprint:*) FINGERPRINTS+=("${line#fingerprint:}") ;;
            *) printf 'warning: unrecognized line in %s: %s\n' "$LOCAL_RULES" "$line" >&2 ;;
        esac
    done < "$LOCAL_RULES"
}

report() {
    printf '\n  [%s] %s\n' "$1" "$2"
    FINDINGS=$((FINDINGS + 1))
}

# --- candidate files ---------------------------------------------------------
#
# Each check runs in two passes. A bulk pass reads all files in a few processes
# and names every file that could produce a hit; it may name too many but never
# too few. The per-file pass below then runs only on those files and writes the
# report. The output is the same as checking every file one by one, but the
# process count no longer grows with files times rules.
#
# Candidate lists are newline-delimited and wrapped in newlines, so membership
# is a single shell pattern match with no extra process.

FILES=()
CANDIDATES=""

is_candidate() {
    case "$1" in *$'\n'"$2"$'\n'*) return 0 ;; esac
    return 1
}

# Names the files that contain at least one match for the given grep arguments,
# as a candidate list. Command substitution strips trailing newlines, so the
# wrapping is added after the capture.
grep_candidates() {
    local found
    found=$(printf '%s\0' "${FILES[@]}" | xargs -0 grep -l "$@" 2>/dev/null || true)
    CANDIDATES=$'\n'"$found"$'\n'
}

# Box-drawing and arrows are used in the architecture diagrams. Anything else
# outside ASCII in a document is likely pasted from an internal source.
#
# Given one file, lists each offending character with its line number. Given
# `--which` and many files, names the files that have at least one.
NON_ASCII_PY='
import sys, unicodedata
allowed = set("\u2500\u2502\u250c\u2510\u2514\u2518\u252c\u2534\u253c\u251c\u2524"
              "\u25b6\u25c0\u25bc\u25b2\u2014\u2013\u2208\u2260\u2264\u2265\u00d7")
def offenders(path):
    for n, line in enumerate(open(path, encoding="utf-8"), 1):
        for ch in line:
            if ord(ch) > 127 and ch not in allowed:
                name = unicodedata.name(ch, "unnamed")
                yield "%d: U+%04X %s" % (n, ord(ch), name)
if sys.argv[1] == "--which":
    for path in sys.argv[2:]:
        try:
            if next(offenders(path), None) is not None:
                print(path)
        except Exception:
            print(path)  # let the per-file pass report the failure
else:
    for line in offenders(sys.argv[1]):
        print(line)
'

# Names the files holding a token whose SHA-256 is a known fingerprint. Uses
# the same token shape as scan_fingerprints.
FINGERPRINT_PY='
import hashlib, os, re, sys
known = set(os.environ["SCAN_FINGERPRINTS"].split())
token = re.compile(rb"[A-Za-z0-9_./+=-]{20,}")
for path in sys.argv[1:]:
    try:
        with open(path, "rb") as f:
            tokens = set(token.findall(f.read()))
    except OSError:
        continue
    if any(hashlib.sha256(t).hexdigest() in known for t in tokens):
        print(path)
'

GENERIC_CANDIDATES=()
LOCAL_CANDIDATES=()
FINGERPRINT_CANDIDATES=""
NON_ASCII_CANDIDATES=""

find_candidates() {
    local i placeholder_files
    grep_candidates -F -e "$ALLOWED_ACCOUNT_PLACEHOLDER"
    placeholder_files="$CANDIDATES"

    for i in "${!RULES[@]}"; do
        local rule="${RULES[$i]}"
        grep_candidates -EI -e "${rule#*|}"
        GENERIC_CANDIDATES[i]="$CANDIDATES"
        # Neutralizing the placeholder can create a match the raw text lacks,
        # so every file that contains the placeholder is a candidate too.
        case "${rule%%|*}" in
            *"account number"*)
                GENERIC_CANDIDATES[i]="${GENERIC_CANDIDATES[$i]}${placeholder_files}" ;;
        esac
    done

    for i in ${LOCAL_PATTERNS+"${!LOCAL_PATTERNS[@]}"}; do
        grep_candidates -EIi -e "${LOCAL_PATTERNS[$i]}"
        LOCAL_CANDIDATES[i]="$CANDIDATES"
    done

    local have_python=0
    command -v python3 >/dev/null 2>&1 && have_python=1

    # If the bulk pass cannot run, every file is a candidate: slower, never blind.
    local found
    if [ ${#FINGERPRINTS[@]} -gt 0 ]; then
        if [ "$have_python" = "1" ] && found=$(printf '%s\0' "${FILES[@]}" \
            | SCAN_FINGERPRINTS="${FINGERPRINTS[*]}" xargs -0 python3 -c "$FINGERPRINT_PY" \
            2>/dev/null); then
            FINGERPRINT_CANDIDATES=$'\n'"$found"$'\n'
        else
            FINGERPRINT_CANDIDATES=$'\n'"$(printf '%s\n' "${FILES[@]}")"$'\n'
        fi
    fi

    local md_files=() file
    for file in "${FILES[@]}"; do
        case "$file" in *.md) md_files+=("$file") ;; esac
    done
    if [ ${#md_files[@]} -gt 0 ]; then
        if [ "$have_python" = "1" ] && found=$(printf '%s\0' "${md_files[@]}" \
            | xargs -0 python3 -c "$NON_ASCII_PY" --which 2>/dev/null); then
            NON_ASCII_CANDIDATES=$'\n'"$found"$'\n'
        else
            NON_ASCII_CANDIDATES=$'\n'"$(printf '%s\n' "${md_files[@]}")"$'\n'
        fi
    fi
}

# --- scanning ----------------------------------------------------------------

scan_patterns() {
    local file="$1"
    local i

    for i in "${!RULES[@]}"; do
        is_candidate "${GENERIC_CANDIDATES[$i]}" "$file" || continue
        local rule="${RULES[$i]}"
        local description="${rule%%|*}"
        local pattern="${rule#*|}"
        local hits

        # Rules that match on an account-number shape are evaluated against a copy
        # in which the documented placeholder has been neutralized. Neutralizing
        # rather than dropping whole lines means a real account sharing a line with
        # the placeholder is still reported.
        #
        # `-e` because a pattern may start with a dash, as the private key rule does.
        case "$description" in
            *"account number"*)
                hits=$(sed "s/${ALLOWED_ACCOUNT_PLACEHOLDER}/PLACEHOLDER/g" "$file" \
                    | grep -nEI -e "$pattern" 2>/dev/null || true)
                ;;
            *)
                hits=$(grep -nEI -e "$pattern" "$file" 2>/dev/null || true)
                ;;
        esac

        if [ -n "$hits" ]; then
            report "$description" "$file"$'\n'"$(indent "$hits")"
        fi
    done

    # The example rules file documents the *shape* of origin-specific terms, so a
    # local denylist will match its illustrations. Generic rules still apply to it.
    if [ "$file" = "$LOCAL_RULES.example" ]; then
        return 0
    fi

    for i in ${LOCAL_PATTERNS+"${!LOCAL_PATTERNS[@]}"}; do
        is_candidate "${LOCAL_CANDIDATES[$i]}" "$file" || continue
        local hits
        hits=$(grep -nEIi -e "${LOCAL_PATTERNS[$i]}" "$file" 2>/dev/null || true)
        if [ -n "$hits" ]; then
            report "local rule" "$file"$'\n'"$(indent "$hits")"
        fi
    done

    # Explicit: a trailing test that finds nothing would otherwise make this
    # function return non-zero and, under `set -e`, abort the whole scan.
    return 0
}

# Hashes every token long enough to be a credential and compares against known
# fingerprints. This detects a specific secret without this repository, or this
# script, ever containing its value.
scan_fingerprints() {
    local file="$1"
    [ ${#FINGERPRINTS[@]} -eq 0 ] && return 0
    is_candidate "$FINGERPRINT_CANDIDATES" "$file" || return 0

    local token digest
    while IFS= read -r token; do
        digest=$(printf '%s' "$token" | shasum -a 256 | cut -d' ' -f1)
        for known in "${FINGERPRINTS[@]}"; do
            if [ "$digest" = "$known" ]; then
                report "known credential" "$file (token fingerprint ${known:0:12}...)"
            fi
        done
    done < <(grep -oE '[A-Za-z0-9_./+=-]{20,}' "$file" 2>/dev/null | sort -u || true)

    return 0
}

scan_non_ascii() {
    local file="$1"
    case "$file" in *.md) ;; *) return 0 ;; esac
    is_candidate "$NON_ASCII_CANDIDATES" "$file" || return 0

    local offenders
    offenders=$(python3 -c "$NON_ASCII_PY" "$file" || true)
    if [ -n "$offenders" ]; then
        report "unexpected non-ASCII" "$file"$'\n'"$(indent "$offenders")"
    fi

    return 0
}

# --- main --------------------------------------------------------------------

if [ "${1:-}" = "--list" ]; then
    printf 'Generic rules:\n'
    for rule in "${RULES[@]}"; do printf '  - %s\n' "${rule%%|*}"; done
    load_local_rules
    printf 'Local rules loaded from %s: %d pattern(s), %d fingerprint(s)\n' \
        "$LOCAL_RULES" "${#LOCAL_PATTERNS[@]}" "${#FINGERPRINTS[@]}"
    exit 0
fi

load_local_rules

if [ ! -f "$LOCAL_RULES" ]; then
    printf 'note: %s not found — scanning with generic rules only.\n' "$LOCAL_RULES" >&2
    printf '      See %s.example to enable credential and project-term checks.\n\n' \
        "$LOCAL_RULES" >&2
fi

while IFS= read -r file; do
    is_excluded "$file" && continue
    [ -f "$file" ] || continue
    FILES+=("$file")
done < <(git ls-files --cached --others --exclude-standard)

if [ ${#FILES[@]} -gt 0 ]; then
    find_candidates
    for file in "${FILES[@]}"; do
        scan_patterns "$file"
        scan_fingerprints "$file"
        scan_non_ascii "$file"
    done
fi

printf '\nScanned %d file(s).\n' "${#FILES[@]}"

if [ "$FINDINGS" -gt 0 ]; then
    printf '%d finding(s). Do not commit until each is resolved.\n' "$FINDINGS" >&2
    printf 'Removing a value in a later commit does not help: git keeps history.\n' >&2
    exit 1
fi

printf 'No findings.\n'
