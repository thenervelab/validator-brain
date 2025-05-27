run rabbitmq for user profile and fill the queue with user profile data: python rabbitmq/user_profile_processor.py

python rabbitmq/miner_profile_processor.py 





To start the profile consumer and indexer:

the processor will fill the queue with user profile data:

```
python rabbitmq/user_profile_processor.py

```

then start a processor ( or multiples )

```

   DATABASE_URL=postgresql://user:password@localhost:5432/substrate_fetcher python rabbitmq/user_profile_consumer.py

```



Rabbitmq for storage requests:

``` 
python rabbitmq/pinning_request_processor.py 
```


and to consume:

```
DATABASE_URL=postgresql://user:password@localhost:5432/substrate_fetcher python rabbitmq/pinning_request_consumer.py
```
