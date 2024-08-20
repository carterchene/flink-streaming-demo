CREATE TABLE auth_events (
    `ts` BIGINT, `sessionId` STRING, `level` STRING, `itemInSession` INT,
    `city` STRING, `zip` STRING, `state` STRING, `userAgent` STRING,
    `lon` DOUBLE, `lat` DOUBLE, `userId` STRING, `lastName` STRING,
    `firstName` STRING, `gender` STRING, `registration` BIGINT, `success` BOOLEAN,
    `year` STRING,
    `month` STRING,
    `day` STRING,
    `hour` STRING
) PARTITIONED BY (`year`, `month`, `day`, `hour`) WITH (
    'connector' = '{{connector}}',
    'path' = '{{path}}',
    'format' = '{{format}}',
    'sink.rolling-policy.file-size' = '{{file_size}}',
    'sink.rolling-policy.rollover-interval' = '{{rollover_interval}}',
    'sink.rolling-policy.check-interval' = '{{check_interval}}',
    'partition.time-extractor.timestamp-pattern'='{{partition_pattern}}',
    'sink.partition-commit.delay'='{{commit_delay}}',
    'sink.partition-commit.policy.kind'='{{commit_policy}}',
    'sink.partition-commit.trigger' = '{{commit_trigger}}',
    'sink.partition-commit.watermark-time-zone' = '{{time_zone}}'
)