#!/bin/sh

echo example 1 using arrow-cat
./rs-docker-networks2arrow-ipc \
	--docker-sock-path ~/docker.sock \
	--docker-conn-timeout 10 |
	arrow-cat |
	tail -3

echo
echo example 2 using sql
./rs-docker-networks2arrow-ipc \
	--docker-sock-path ~/docker.sock \
	--docker-conn-timeout 10 |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'docker_networks' \
	--sql "
		SELECT
			*
		FROM docker_networks
		WHERE
			name NOT LIKE 'f%'
			AND name NOT LIKE 'z%'
			AND name NOT LIKE 's%'
		LIMIT 10
	" |
	rs-arrow-ipc-stream-cat
