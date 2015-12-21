#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting SchoolScope in background, redirected to /dev/null."
echo "Kill by manually killing sbtester_app_server and sbtester_content_server."
${DIR}/../src/schoolbus_test_server/sbtester_app_server.py > /dev/null 2>&1 &
