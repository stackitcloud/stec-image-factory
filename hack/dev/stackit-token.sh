#!/bin/sh

### Print a short-lived STACKIT access token for a service account key.
###
### This is the key flow described in the STACKIT docs ("Acquire short lived access tokens"):
### a JWT signed with the key's private RSA key is exchanged at the token endpoint for a
### bearer token. The STACKIT SDK does this on its own, so the image factory does not need
### this script. It is for the curl calls around it, for example to create the KMS key ring
### and to encrypt the SecureBoot key files.
###
### Usage: hack/dev/stackit-token.sh [service-account-key.json]
###        The path defaults to STACKIT_SERVICE_ACCOUNT_KEY_PATH.
###
### Needs jq, openssl, uuidgen and curl. The token is printed to stdout and nothing else.

set -eu

KEY_FILE="${1:-${STACKIT_SERVICE_ACCOUNT_KEY_PATH:-}}"

die() {
    echo "error: $*" >&2
    exit 1
}

if [ -z "${KEY_FILE}" ]; then
    die "usage: $0 <service-account-key.json>"
fi

if [ ! -r "${KEY_FILE}" ]; then
    die "cannot read ${KEY_FILE}"
fi

base64url() {
    openssl base64 -A | tr '+/' '-_' | tr -d '='
}

PRIVATE_KEY="$(mktemp)"
trap 'rm -f "${PRIVATE_KEY}"' EXIT INT TERM

jq -r '.credentials.privateKey' "${KEY_FILE}" > "${PRIVATE_KEY}"

ISS="$(jq -r '.credentials.iss' "${KEY_FILE}")"
SUB="$(jq -r '.credentials.sub' "${KEY_FILE}")"
AUD="$(jq -r '.credentials.aud' "${KEY_FILE}")"
KID="$(jq -r '.credentials.kid' "${KEY_FILE}")"
TOKEN_URL="$(jq -r '.credentials.tokenEndpoint' "${KEY_FILE}")"

NOW="$(date +%s)"
EXP=$((NOW + 600))
JTI="$(uuidgen)"

HEADER="$(jq -cn --arg kid "${KID}" '{alg: "RS512", typ: "JWT", kid: $kid}' | base64url)"
PAYLOAD="$(jq -cn --arg iss "${ISS}" --arg sub "${SUB}" --arg aud "${AUD}" --arg jti "${JTI}" \
    --argjson iat "${NOW}" --argjson exp "${EXP}" \
    '{iss: $iss, sub: $sub, aud: $aud, jti: $jti, iat: $iat, exp: $exp}' | base64url)"
SIGNATURE="$(printf '%s.%s' "${HEADER}" "${PAYLOAD}" \
    | openssl dgst -sha512 -sign "${PRIVATE_KEY}" -binary | base64url)"

RESPONSE="$(curl --fail -sS -X POST "${TOKEN_URL}" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer' \
    --data-urlencode "assertion=${HEADER}.${PAYLOAD}.${SIGNATURE}")"

TOKEN="$(printf '%s' "${RESPONSE}" | jq -r '.access_token // empty')"

if [ -z "${TOKEN}" ]; then
    die "no access_token in response: ${RESPONSE}"
fi

printf '%s\n' "${TOKEN}"
