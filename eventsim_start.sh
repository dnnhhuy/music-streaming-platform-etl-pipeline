docker run -it \
  --network music_streaming_event_default \
  --name eventsim \
  --memory="5.5g" \
  --memory-swap="7g" \
  eventsim \
    -c "examples/alt-example-config.json" \
    --start-time "`date +"%Y-%m-%dT%H:%M:%S"`" \
    --end-time "`date -v "+1d" +"%Y-%m-%dT%H:%M:%S"`" \
    --nusers 1000000 \
    --growth-rate 0.3 \
    --userid 1 \
    --kafkaBrokerList kafka:9092 \
    --randomseed 1 \
    --continuous

  
docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 /src/stream_process.py