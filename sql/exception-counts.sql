-- Record count by exception type
SELECT e.task_status, e.exception, count(1)
FROM twelve_labs_vector
CROSS JOIN UNNEST(embedding) as t(e)
where e.task_status!='ready'
group by 1, 2