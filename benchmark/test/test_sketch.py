from itertools import count, repeat

from twisted.internet.defer import Deferred, succeed, fail
from twisted.trial.unittest import TestCase

from benchmark.benchmark_sketch import sample


class FakeMeasurement:

    def __init__(self, measurements):
        """
        :param measurements: An iterator providing measurement to be
            returned on each call.
        """
        self.measurements = measurements

    def __call__(self, f, *a, **kw):
        def finished(ignored):
            return next(self.measurements)

        d = f(*a, **kw)
        d.addCallback(finished)
        return d


class FakeProbe:
    """
    A probe performs a single operation, which can be timed.
    """
    def __init__(self, good):
        self.good = good

    def run(self):
        if self.good:
            return succeed(None)
        else:
            return fail(RuntimeError('ProbeFailed'))

    def cleanup(self):
        return succeed(None)


class FakeMetric:
    """
    A metric returns a probe to measure the metric. To ensure sequential
    operations perform real work, the metric may return a different
    probe each time.
    """

    def __init__(self, succeeds):
        """
        :param succeeds: An iterator providing a boolean indicating
            whether the probe will succeed.
        """
        self.succeeds = succeeds

    def get_probe(self):
        return FakeProbe(next(self.succeeds))


class FakeScenario:

    def start(self):
        return succeed(None), Deferred()

    def stop(self):
        return succeed(None)


class SampleTest(TestCase):

    def test_good_probes(self):
        """
        Sampling returns results when probes succeed.
        """
        samples_ready = sample(
            FakeMeasurement(count(5)),
            FakeMetric(repeat(True)),
            FakeScenario(),
            3)

        def check(samples):
            self.assertEqual(
                samples, [{'success': True, 'value': x} for x in [5, 6, 7]])
        samples_ready.addCallback(check)
        return samples_ready

    def test_bad_probes(self):
        """
        Sampling returns reasons when probes fail.
        """
        samples_ready = sample(
            FakeMeasurement(count(5)),
            FakeMetric(repeat(False)),
            FakeScenario(),
            3)

        def check(samples):
            # We don't care about the actual value for reason.
            for s in samples:
                if 'reason' in s:
                    s['reason'] = None
            self.assertEqual(
                samples,
                [{'success': False, 'reason': None} for x in [5, 6, 7]])
        samples_ready.addCallback(check)
        return samples_ready
