#!/usr/bin/env bash
HOME_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
AUTOSCALER_RUN_DIR="$HOME_DIR/AutoScaler/"
MANAGER_RUN_DIR="$HOME_DIR/ManagerApp/" 
FRONTEND_RUN_DIR="$HOME_DIR/FrontendApp/" 
REACTWEB_RUN_DIR="$HOME_DIR/ReactJS/" 

echo "The HOME DIR for Apps is '$HOME_DIR'"

echo "Starting the AutoScaler"
python3 ${AUTOSCALER_RUN_DIR}autoscaler.py >/dev/null &
echo "Waiting the AutoScaler to start"
sleep 2

echo "Starting the FrontendApp"
python3 ${FRONTEND_RUN_DIR}app.py &
echo "Waiting the FrontendApp to start"
sleep 3

echo "Starting the ManagerApp"
python3 ${MANAGER_RUN_DIR}run.py &
echo "Waiting the ManagerApp to start"
sleep 3

echo "Starting the React JS"
npm start --prefix ${REACTWEB_RUN_DIR}
