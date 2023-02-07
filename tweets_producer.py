"""Module to Produce Tweets to Kafka topics"""
import json
import os
import time

import tweepy
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tweepy import StreamRule

from twitter_analytics_logger import logger

politicians_rules_config = {
    "zelensky": "context:35.1498702169922420739",
    "putin": "context:35.864931126132985856",
    "biden": "context:35.10040395078",
    "nato": "context:88.1016738021939351558",
    "noflyzone": "(#NoFlyZone OR #CloseTheSky OR #CloseTheSkyOverUkraine)",
    "query": "(#RussianUkrainianWar OR #UkraineWar OR #Russia OR #Ukraine) "
    "lang:en -is:retweet -nft -crypto -bitcoin ",
    "bearer_token": os.environ.get("TWITTER_TOKEN"),
}
politicians_tags = ["Zelensky", "Putin", "Biden", "NATO", "NoFlyZone"]
politicians_query = politicians_rules_config.get("query")
politicians_rules = [
    f"{politicians_rules_config.get('zelensky')} {politicians_query}",
    f"{politicians_rules_config.get('putin')} {politicians_query}",
    f"{politicians_rules_config.get('biden')} {politicians_query}",
    f"{politicians_rules_config.get('nato')} {politicians_query}",
    f"{politicians_rules_config.get('noflyzone')} {politicians_query}",
]


class KafkaProducer:
    """
    Kafka producer class wrapper
    to interact with topics and messages
    """

    def __init__(self, kafka_topics):
        self.kafka_servers = "localhost:9092,localhost:9093,localhost:9094"
        self.kafka_config = {
            "bootstrap.servers": self.kafka_servers,
            "partitioner": "consistent_random",
        }
        self.kafka_producer = Producer(self.kafka_config)
        self.kafka_admin_client = AdminClient({"bootstrap.servers": self.kafka_servers})
        self.kafka_topics = sorted(kafka_topics)
        self._create_topics()

    def _create_topics(self):
        existed_topics = self.__get_existed_topics()

        if self.kafka_topics == existed_topics:
            logger.info(f"Topics already exists - {existed_topics}")
            return

        kafka_topics = self.__setup_topics(self.kafka_topics)
        topics_futures = self.kafka_admin_client.create_topics(kafka_topics)

        for topic, future in topics_futures.items():
            future.result()
            logger.info(f"Topic {topic} created")

    @staticmethod
    def __setup_topics(topic_names: list, partitions=3, replication_factor=3):
        kafka_topics = [
            NewTopic(
                topic, num_partitions=partitions, replication_factor=replication_factor
            )
            for topic in topic_names
        ]
        return kafka_topics

    def __get_existed_topics(self):
        return sorted(list(self.kafka_admin_client.list_topics().topics.keys()))

    def produce_message(self, topic, value, tweet_id) -> None:
        """Method to send messages to Kafka Topic"""
        key = f"{topic[:2].upper()}{tweet_id}".encode("utf-8")
        logger.info(f"Sending {value} to {topic}. Key - {key}")
        self.kafka_producer.produce(topic=topic, value=value, key=key)


class TwitterListener(tweepy.StreamingClient):
    """
    Tweepy Streaming Client class wrapper
    to interact with streaming tweets raw data
    """

    def __init__(self, bearer_token, **kwargs):
        super().__init__(bearer_token, **kwargs)
        self.twitter_kafka_producer = KafkaProducer(politicians_tags)
        self.delete_existed_rules()
        self.add_new_rules(politicians_rules, politicians_tags)
        self.filter(tweet_fields=["created_at"])

    def on_data(self, raw_data):
        logger.info(f"Raw data received - {raw_data}")
        message = json.loads(raw_data)
        if matching_rules := message.get("matching_rules"):
            logger.info(f"Got matching rules in on_data method {matching_rules}")
            for stream_rule in matching_rules:
                self.twitter_kafka_producer.produce_message(
                    topic=stream_rule["tag"],
                    value=raw_data,
                    tweet_id=message["data"]["id"],
                )
        else:
            logger.error("Operational error, reconnecting...")
        return True

    def on_tweet(self, tweet):
        logger.info(f"Tweet received - {tweet}")

    def on_includes(self, includes):
        logger.info(f"Includes are received - {includes}")

    def on_errors(self, errors):
        logger.error(f"Error received - {errors}")

    def on_matching_rules(self, matching_rules):
        logger.info(f"Matching rules received - {matching_rules}")

    def on_response(self, response):
        logger.info(f"Response received - {response}")

    def on_closed(self, response):
        logger.info(f"Stream has been closed by Twitter. Response - {response}")

    def on_connect(self):
        logger.info("Successfully connected")

    def on_connection_error(self):
        logger.error("Stream connection errors on times out")

    def on_disconnect(self):
        logger.info("Stream has disconnected")

    def on_exception(self, exception):
        logger.exception(f"Unhandled exception occurs {exception}")

    def on_keep_alive(self):
        logger.info("Keep-alive signal is received")

    def on_request_error(self, status_code):
        logger.error(
            f"Non-200 HTTP status code is encountered. Status_code - {status_code}"
        )
        if status_code == 429:
            logger.info("Disconnecting streaming client")
            self.disconnect()

    def delete_existed_rules(self):
        """
        Method to delete existed rules from Twitter API application
        :return:
        """
        existed_rules = self.get_rules().data
        if existed_rules:
            for rule in existed_rules:
                logger.info(
                    f"Deleting existed rule (tag - {rule.tag}) with value {rule.value}"
                )
                self.delete_rules(rule.id)
                time.sleep(1)

    def add_new_rules(self, rules, tags):
        """
        Method to add rules for Twitter API application
        :param rules:
        :param tags:
        :return:
        """
        for rule, tag in zip(rules, tags):
            logger.info(f"Adding rule {rule} with tag {tag}")
            response = self.add_rules(StreamRule(value=rule, tag=tag))
            logger.info(f"Added rule response - {response}")
            time.sleep(1)


if __name__ == "__main__":
    twitter_listener = TwitterListener(
        politicians_rules_config.get("bearer_token"),
        wait_on_rate_limit=True,
        max_retries=3,
    )
