# Web Traffic Sessionization

## Overview
This project processes web traffic data by assigning a unique `session_id` to user sessions based on a 30-minute inactivity rule. It also counts the number of sessions containing a specific URL.

## Problem Statement
- A **new session** starts if a pageview occurs more than **30 minutes** after the previous pageview from the same `anon_id`.
- Otherwise, it belongs to the same session.

## Solution Summary
- **Session Assignment**: Uses window functions to compare timestamps per user and increment a session counter when the 30-minute gap is exceeded.
- **Session Counting**: Filters sessions that contain a specific URL and counts unique session IDs.

## Incremental Processing Considerations
- **Efficient Data Loading**: Maintain session state for active users.
- **Challenges**: Handling late-arriving data, high-frequency users, and optimizing data partitioning.

## Repository Files
- `1-start-here.txt` – Project description.
- `2-solution.sql` – SQL query for sessionization.
- `3-alternative-solution.py` – Python alternative.
- `test1.csv` – Sample dataset.

This project supports **web analytics, user behavior tracking, and recommendation systems**.