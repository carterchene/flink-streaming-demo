CREATE TABLE kafka_status_change_events (
    `ts` BIGINT
    , `sessionId` STRING
    , `auth` STRING
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
) WITH (
    'connector' = '{{connector}}',
    'topic' = '{{topic}}',
    'properties.bootstrap.servers' = '{{bootstrap_servers}}',
    'properties.group.id' = '{{consumer_group_id}}',
    'scan.startup.mode' = '{{scan_startup_mode}}',
    'format' = '{{format}}'
)
