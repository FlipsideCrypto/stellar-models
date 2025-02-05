{% docs batch_insert_ts %}
The timestamp in UTC when a batch of records was inserted into the database. This field can help identify if a batch executed in real time or as part of a backfill. The timestamp should not be used during ad hoc analysis and is useful for data engineering purposes.
{% enddocs %}
