docker run -itd \
  --network music_streaming_event_default \
  --name million_events \
  --memory="5.5g" \
  --memory-swap="7g" \
  music_events \
    -c "examples/example-config.json" \
    --start-time "`date +"%Y-%m-%dT%H:%M:%S"`" \
    --end-time "`date -v +1d +"%Y-%m-%dT%H:%M:%S"`" \
    --nusers 1000000 \
    --growth-rate 10 \
    --userid 1 \
    --kafkaBrokerList kafka:9092 \
    --randomseed 1 \
    --continuous