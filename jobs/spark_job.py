from abc import ABC, abstractmethod


class PysparkJob(ABC):
    def __init__(self, spark, name):
        self.spark = spark
        self.name = name

    @abstractmethod
    def start(self):
        # Implement the start method and return its output
        output = {}
        return output

    @abstractmethod
    def run(self, start_output=None):
        output = {}
        return output

    @abstractmethod
    def end(self, run_output=None):
        # Implement the end method using the previous_output
        end_output = {}
        return end_output

    def execute(self):
        start_output = self.start()
        run_output = self.run(start_output)
        self.end(run_output)