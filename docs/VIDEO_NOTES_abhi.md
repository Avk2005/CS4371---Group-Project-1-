# Video notes: Abhi's part (aim for about 2 minutes of the 8)

## Show on screen
1. The start command: `./run_streaming.sh <model_dir> <checkpoint_dir>`
2. The console: the "top 10 named entities" table and the `predicted_class | count` table
3. Let 2 or 3 triggers pass so the counts visibly grow
4. Optional: `kafka-console-consumer.sh --topic topic2` showing the JSON messages Kibana reads

## Talking points
- "My part is the Spark Structured Streaming app. It reads every new message from topic1 as it arrives."
- "Each message is cleaned first. Hacker News comments come as HTML, so I strip the tags and decode things like &amp;."
- "Query 1 runs spaCy named-entity recognition on each message. It keeps people, organizations,
  places and products, and leaves out numbers and dates, which would otherwise fill the top 10."
- "Spark keeps a running count per entity. Every 30 seconds it sends the top 50 to topic2 as JSON
  with the count, rank and timestamp. Logstash picks that up and Kibana draws the top-10 bar chart."
- "Query 2 loads the PipelineModel that Chris trained on AG News. It's the same tokenizer, stop words,
  TF-IDF and logistic regression, so live messages go through exactly the same steps.
  It keeps a running count of messages per predicted class."
- Point out what you see: "Sci/Tech dominates, which makes sense for Hacker News. Sports barely shows up."
- "Both queries read the same topic at the same time, and checkpoints let them restart without losing counts."

## Screenshots for the report
- The class-count console table at a few points, for example 15, 30, 45 and 60 minutes, next to Kareem's Kibana shots
- One screenshot of the top-10 entity console table
