# kixi.comms

Library which provides a common communications model (protocol & implementations) for microservices inside the Kixi architecture

## Tests

To run tests, you need Kafka and Zookeeper running which are provided by `docker-compose.yml`

``` bash
docker-compose up -d
... wait a few seconds ...
lein test
```

## License

Copyright © 2016 Mastodon C

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
