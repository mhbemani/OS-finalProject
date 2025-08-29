# OS-finalProject
producer-consumer synchronization system



- you can compile the code simply by running:
    gcc -std=c11 -O2 -pthread -o broker multithreaded_message_broker.c

- then to run the compiled code with default values, just run this command:
    ./broker

- but if you wanted to pass a new value as a system config, you can add these labels
  to your running command:

    --seconds=N: Runtime duration in seconds (default: 5)

    --producers=N: Number of producer threads (default: 5)

    --consumers=N: Number of consumer threads (default: 5)

    --topics=N: Number of topics (default: 3)

    --bounded: Enable bounded queues

    --capacity=N: Queue capacity when bounded

    --retention: Enable message retention with TTL

    --ttl=N: Time-to-live for messages in ms

    --gc-interval=N: Garbage collection interval in ms

    --fanout: Enable fanout mode (pub/sub)

- example: 
    ./broker --seconds=10 --producers=3 --consumers=3 --topics=2
