#!/bin/bash 

sudo apt install virtualenv

python3 -m virtualenv venv
venv/bin/pip3 install -r requirements.txt

cp config.ini.sample config.ini
sudo chmod +x boot.sh

echo "Setup complete. Please edit config.ini with your settings."