CREATE TABLE kafka_page_view_events (
      `ts` BIGINT
    , `sessionId` STRING
    , `page` STRING
    , `auth` STRING
    , `method` STRING
    , `status` INT
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
    , `artist` STRING
    , `song` STRING
    , `duration` DOUBLE
) WITH (
    'connector' = '{{connector}}',
    'topic' = '{{topic}}',
    'properties.bootstrap.servers' = '{{bootstrap_servers}}',
    'properties.group.id' = '{{consumer_group_id}}',
    'scan.startup.mode' = '{{scan_startup_mode}}',
    'format' = '{{format}}'
)