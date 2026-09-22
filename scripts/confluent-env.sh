#!/bin/bash
# Confluent Cloud CLI environment - Walmart corporate network
#
# PROBLEM: the corporate DNS resolver (172.17.168.10) returns NXDOMAIN for
#   confluent.cloud / api.confluent.cloud / docs.confluent.io
# so the CLI cannot even dial the auth endpoint:
#   Error: Post "https://confluent.cloud/api/sessions": dial tcp: no such host
#
# SOLUTION: route through the proxy named by the corporate PAC file
#   http://wmtpac.wal-mart.com/proxies/anycast-universal.pac
# whose universal default is proxy-intlho.wal-mart.com:8080.
#
# VERIFIED 2026-09-22:
#   proxy-intlho.wal-mart.com:8080  -> HTTP 200   <-- use this
#   sysproxy.wal-mart.com:8080      -> HTTP 407 (proxy auth required)
#
# Usage:
#   source scripts/confluent-env.sh
#   confluent login --save

export HTTPS_PROXY=http://proxy-intlho.wal-mart.com:8080
export HTTP_PROXY=http://proxy-intlho.wal-mart.com:8080
export NO_PROXY=localhost,127.0.0.1,.wal-mart.com,.walmart.com

echo "Confluent proxy set -> proxy-intlho.wal-mart.com:8080"
echo
echo "Next:"
echo "  confluent login --save        # email + password"
echo "  confluent login --sso <email> # if your org uses SSO"
