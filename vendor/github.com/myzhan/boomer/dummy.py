# coding: utf8

from locust import User, task

class Dummy(User):
    @task(20)
    def hello(self):
        pass
