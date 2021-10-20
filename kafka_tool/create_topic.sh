#!/bin/bash


# naver news crawling data topic
echo "create naver news topic"
/home/ubuntu/kafka/bin/kafka-topics.sh 	--zookeeper zoo1:2181/gentle_kafka \
					--replication-factor 2 \
					--topic naver_news \
					--partitions 1 \
					--create

# daum news crawling data topic
echo "create daum news topic"
/home/ubuntu/kafka/bin/kafka-topics.sh  --zookeeper zoo1:2181/gentle_kafka \
                    --replication-factor 2 \
					--topic daum_news \
					--partitions 1 \
					--create

# youtube crawling data topic
echo "create youtube contents topic"
/home/ubuntu/kafka/bin/kafka-topics.sh  --zookeeper zoo1:2181/gentle_kafka \
                    --replication-factor 2 \
					--topic youtube_contents \
					--partitions 1 \
					--create

# google trend ranking data topic
echo "create google trend topic"
/home/ubuntu/kafka/bin/kafka-topics.sh  --zookeeper zoo1:2181/gentle_kafka \
					--replication-factor 2 \
					--topic google_trend \
					--partitions 1 \
					--create

