aws emr create-cluster \
  --name "StackOverflow-Tags-Cluster" \
  --release-label emr-6.15.0 \
  --applications Name=Hadoop \
  --use-default-roles \
  --region us-west-2 \
  --ec2-attributes KeyName=mykeypair \
  --instance-type m5.xlarge \
  --instance-count 4 \
  --auto-terminate \
  --log-uri s3://lp-trend-project/logs/ \
  --steps '[
    {
      "Type": "CUSTOM_JAR",
      "Name": "Task1-TagEmit",
      "ActionOnFailure": "CONTINUE",
      "Jar": "s3://lp-trend-project/jars/Task1TagEmit.jar",
      "Args": [
        "s3://lp-trend-project/Posts.xml",
        "s3://lp-trend-project/outputs/task1-tag-emit"
      ]
    },
    {
      "Type": "CUSTOM_JAR",
      "Name": "Task2-Yearly-Agg",
      "ActionOnFailure": "CONTINUE",
      "Jar": "s3://lp-trend-project/jars/Task2YearlyTagAgg.jar",
      "Args": [
        "agg",
        "s3://lp-trend-project/outputs/task1-tag-emit",
        "s3://lp-trend-project/outputs/task2-yearly-tag-agg"
      ]
    },
    {
      "Type": "CUSTOM_JAR",
      "Name": "Task2-Yearly-TopK",
      "ActionOnFailure": "CONTINUE",
      "Jar": "s3://lp-trend-project/jars/Task2YearlyTagAgg.jar",
      "Args": [
        "topk",
        "s3://lp-trend-project/outputs/task2-yearly-tag-agg",
        "s3://lp-trend-project/outputs/task2-yearly-topk",
        "20"
      ]
    }
  ]'
