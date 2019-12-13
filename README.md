# S3 Helper

Adapted from https://github.com/dknochen/xaws for accessing S3 buckets with eXist-db

Not intended for general use yet

Supply your AWS credentials in modules/aws_config.xqm (duplicate the template file, rename to aws_config.xqm)

## Build

1. Single `xar` file: The `collection.xconf` will only contain the index, not any triggers!
    ~~~shell
    ant
    ~~~

2. DEV environment: The replication triggers for the producer server are enabled in  `collection.xconf` and point to the dev server's replication service IP.
    ~~~shell
    ant xar-dev
    ~~~

3. PROD environment: Same as in 2. but for PROD destination
    ~~~shell
    ant xar-prod
    ~~~
