CREATE TABLE kafka_listen_events (
      `song` STRING
    , `artist` STRING
    , `duration` DOUBLE
    , `ts` BIGINT
    , `sessionId` INTEGER
    , `auth` STRING
    , `level` STRING
    , `itemInSession` INTEGER
    , `city` STRING
    , `zip` STRING
    , `state` STRING
    , `userAgent` STRING
    , `lon` DOUBLE
    , `lat` DOUBLE
    , `userId` INTEGER
    , `lastName` STRING
    , `firstName` STRING
    , `gender` STRING
    , `registration` BIGINT
) WITH (
    'connector' = '{{connector}}',
    'topic' = '{{topic}}',
    'properties.bootstrap.servers' = '{{bootstrap_servers}}',
    'properties.group.id' = '{{consumer_group_id}}',
    'scan.startup.mode' = '{{scan_startup_mode}}',
    'format' = '{{format}}'
)