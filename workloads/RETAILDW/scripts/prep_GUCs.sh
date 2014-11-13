#!/bin/bash

# prep_GUCs.sh

# This script changes any default GUCs for optimal demo performance

gpconfig -c max_connections -v 500 -m 100
gpconfig -c gp_segment_connect_timeout -v 600
gpstop -u
