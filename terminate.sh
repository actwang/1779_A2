#!/usr/bin/env bash
echo "Killing the ManagerApp"
fuser -k -n tcp 5001
echo "Killing the AutoScaler"
fuser -k -n tcp 8000
echo "Killing the FrontendApp"
fuser -k -n tcp 5000
echo "Killing the React JS"
fuser -k -n tcp 5002
