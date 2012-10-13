from scrapy.exceptions import DropItem
from utils import resp2item
from pipelines import MongodbPipeline


class Weibo2Db(object):
    def __init__(self):
        self.pipeline = MongodbPipeline()

    def statuses(self, statuses):
        for status in statuses:
            try:
                user, weibo, retweeted_user = resp2item(status)
            except DropItem:
                continue

            self.pipeline.process_item(user, None)
            self.pipeline.process_item(weibo, None)
            if retweeted_user is not None:
                self.pipeline.process_item(retweeted_user, None)
