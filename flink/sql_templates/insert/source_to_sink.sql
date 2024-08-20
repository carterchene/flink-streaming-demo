INSERT INTO {{sink}} 
SELECT 
    *, 
    DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'yyyy') as `year`,
    DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'MM') as `month`,
    DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'dd') as `day`,
    DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'HH') as `hour`
FROM {{source}}