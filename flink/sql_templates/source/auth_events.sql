CREATE TABLE kafka_auth_events (
      `ts` BIGINT
    , `sessionId` STRING
    , `level` STRING
    , `itemInSession` INT
    , `city` STRING
    , `zip` STRING
    , `state` STRING
    , `userAgent` STRING
    , `lon` DOUBLE
    , `lat` DOUBLE
    , `userId` STRING
    , `lastName` STRING
    , `firstName` STRING
    , `gender` STRING
    , `registration` BIGINT
    , `success` BOOLEAN
) WITH (
    'connector' = '{{connector}}',
    'topic' = '{{topic}}',
    'properties.bootstrap.servers' = '{{bootstrap_servers}}',
    'properties.group.id' = '{{consumer_group_id}}',
    'scan.startup.mode' = '{{scan_startup_mode}}',
    'format' = '{{format}}'
)
