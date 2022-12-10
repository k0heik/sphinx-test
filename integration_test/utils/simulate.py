from collections import deque

from utils.testcase import TestCase, PerformanceHistory


class Simulator:
    def __init__(self, testcase: TestCase):
        self.testcase = testcase
        # self.params = testcase.params
        # self.med_wp = testcase.med_winning_price
        self._hist = deque()

    def create(self):
        for i in range(self.testcase.history_days):
            for ad in self.testcase.ad.list:
                self._hist.append(PerformanceHistory(
                    day=i,
                    performance=ad.performance
                ))

    @property
    def performances(self):
        return list(self._hist)
