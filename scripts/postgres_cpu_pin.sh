#!/bin/bash

postmaster_pid=$(pidof postgres | xargs -n1 | sort | head -n1)
sudo taskset -pc 56-83,168-195 $postmaster_pid
pidof postgres -o $postmaster_pid | sudo xargs -n1 taskset -pc 56-83,168-195
