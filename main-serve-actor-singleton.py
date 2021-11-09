import asyncio
import io
import time

import pandas as pd
import ray
from ray import serve
from starlette.requests import Request
from whylogs.core.datasetprofile import DatasetProfile

ray.init()
serve.start()

# TODO Is global singleton state like this an antipattern?
# TODO Is there definitely only a single instance of this state ever?


@ray.remote
class SingletonProfile:
    def __init__(self) -> None:
        self.profile = DatasetProfile("")
        self.profile_queue = asyncio.Queue()
        asyncio.create_task(self.read_profiles())

    async def read_profiles(self):
        while True:
            profile = await self.profile_queue.get()
            self.profile = self.profile.merge(profile)

    def enqueue_profile(self, profile: DatasetProfile):
        asyncio.create_task(self.profile_queue.put(profile))

    def get_summary(self):
        return str(self.profile.to_summary())


singleton = SingletonProfile.remote()


@serve.deployment()
class Logger:
    def log(self, df: pd.DataFrame):
        profile = DatasetProfile("")
        profile.track_dataframe(df)
        ray.get(singleton.enqueue_profile.remote(profile))

    async def __call__(self, request: Request):
        return ray.get(singleton.get_summary.remote())


@serve.deployment
class MyModel:
    def __init__(self) -> None:
        self.logger = Logger.get_handle(sync=True)

    def predict(self, df: pd.DataFrame):
        # implement with a real model
        return []

    async def __call__(self, request: Request):
        bytes = await request.body()
        csv_text = bytes.decode(encoding='UTF-8')
        df = pd. read_csv(io.StringIO(csv_text))
        # log the data with whylogs asynchronously
        self.logger.log.remote(df)
        return self.predict(df)


Logger.deploy()
MyModel.deploy()

while True:
    time.sleep(5)
