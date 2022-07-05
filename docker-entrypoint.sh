#! /bin/sh

# docker-entrypoint for opensemanticsearch/open-semantic-etl

# wait for the apps container to finish initializing:
while ! curl -m 1 -sf http://apps >/dev/null 2>&1
do
	sleep 1
done

exec /usr/bin/python3 /usr/lib/python3/dist-packages/opensemanticetl/tasks.py
