import ray
from typing import List
from ray.data.dataset_pipeline import DatasetPipeline

data_files = ["data/data1.csv", "data/data2.csv", "data/data3.csv"]


@ray.remote
class RemotePipelineActor:
    def __init__(self, pipeline: DatasetPipeline) -> None:
        self.pipeline = pipeline

    def log_from_pipeline(self) -> List[bytes]:
        for df in self.pipeline.iter_batches(batch_size=1000, batch_format="pandas"):
            pass

        return 1


def main_pipeline_actor():
    pipelines = ray.data.read_csv(data_files).pipeline(parallelism=3).split(3)

    actors = [RemotePipelineActor.remote(pipeline) for pipeline in pipelines]
    results = ray.get([actor.log_from_pipeline.remote() for actor in actors])

    return results


if __name__ == "__main__":
    ray.init()
    main_pipeline_actor()
