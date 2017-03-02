# kixi.comms

Library which provides a common communications model (protocol & implementations) for microservices inside the Kixi architecture

## Tests

To run tests, you need Kafka and Zookeeper running which are provided by `docker-compose-kafka.yml` and/or Kinesis, which is provided by `docker-compose-kinesis.yml`

``` bash
docker-compose up -d
... wait a few seconds ...
lein test
```

If you're using Kinesalite, you will need to use

``` bash
AWS_CBOR_DISABLE=1 lein test
```

## License

Copyright © 2016 Mastodon C

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
