from sys import stderr
from pprint import pprint
from json import dump
from platform import node, platform
from datetime import datetime
from os import environ, getcwd

from eliot import to_file

from twisted.python.components import proxyForInterface
from twisted.python.filepath import FilePath
from twisted.internet.task import cooperate
from twisted.internet.defer import maybeDeferred, Deferred

from flocker import __version__ as flocker_client_version
from flocker.apiclient import (
    IFlockerAPIV1Client, FakeFlockerClient, FlockerClient,
)
from flocker.common import gather_deferreds

from benchmark_metrics import get_metric
from benchmark_measurements import get_measurement
from benchmark_scenarios import get_scenario


def sample(measure, operation, scenario):
    setting_up, scenario_status = scenario.start()

    samples = []

    def once(i):
        print("Starting sample #{i}".format(i=i))
        d = maybeDeferred(operation.get_probe)

        def got_probe(probe):
            d = measure(probe.run)
            d.addCallbacks(
                lambda interval: samples.append(
                    dict(success=True, value=interval)
                ),
                lambda reason: samples.append(
                    dict(success=False, reason=reason.getTraceback()),
                ),
            )
            d.addCallback(lambda ignored: probe.cleanup())
            return d
        d.addCallback(got_probe)
        return d

    def start_sampling(ignored):
        sampling_complete = Deferred()
        task = cooperate(once(i) for i in range(3))

        # If the scenario collapses, stop sampling
        def stop_sampling_on_scenario_collapse(failure):
            task.stop()
            sampling_complete.errback(failure)
        scenario_status.addErrback(stop_sampling_on_scenario_collapse)

        task.whenDone().addCallback(sampling_complete.callback)

        return sampling_complete

    sampling = setting_up.addCallback(start_sampling)

    def tear_down(result):
        d = scenario.stop()
        d.addCallback(lambda ignored: result)
        return d

    tearing_down = sampling.addBoth(tear_down)

    when_done = tearing_down.addCallback(lambda ignored: samples)

    return when_done


def record_samples(samples, version, metric_name, measurement_name):
    timestamp = datetime.now().isoformat()
    artifact = dict(
        client=dict(
            flocker_version=flocker_client_version,
            date=timestamp,
            working_directory=getcwd(),
            username=environ[b"USER"],
            nodename=node(),
            platform=platform(),
        ),
        server=dict(
            flocker_version=version,
        ),
        measurement=measurement_name,
        metric_name=metric_name,
        samples=samples,
    )
    print("Measurements of {} for {} against {}:".format(
        measurement_name, metric_name, version,
    ))
    pprint(samples)
    filename = "samples-{timestamp}.json+flocker-benchmark".format(
        timestamp=timestamp
    )
    with open(filename, "w") as f:
        dump(artifact, f)


class FastConvergingFakeFlockerClient(
    proxyForInterface(IFlockerAPIV1Client)
):
    def create_dataset(self, *a, **kw):
        result = self.original.create_dataset(*a, **kw)
        self.original.synchronize_state()
        return result

    def move_dataset(self, *a, **kw):
        result = self.original.move_dataset(*a, **kw)
        self.original.synchronize_state()
        return result

    def delete_dataset(self, *a, **kw):
        result = self.original.delete_dataset(*a, **kw)
        self.original.synchronize_state()
        return result

    def create_container(self, *a, **kw):
        result = self.original.create_container(*a, **kw)
        self.original.synchronize_state()
        return result

    def delete_container(self, *a, **kw):
        result = self.original.delete_container(*a, **kw)
        self.original.synchronize_state()
        return result


def driver(reactor, control_service_address=None, cert_directory=b"certs",
           metric_name=b"read-request", measurement_name=b"wallclock",
           scenario_name=b'ten_ro_req_sec'):

    to_file(stderr)
    # from twisted.internet.defer import setDebugging
    # setDebugging(True)
    # from twisted.python.failure import startDebugMode
    # startDebugMode()

    if control_service_address:
        cert_directory = FilePath(cert_directory)
        client = FlockerClient(
            reactor,
            host=control_service_address,
            port=4523,
            ca_cluster_path=cert_directory.child(b"cluster.crt"),
            cert_path=cert_directory.child(b"user.crt"),
            key_path=cert_directory.child(b"user.key"),
        )
    else:
        client = FastConvergingFakeFlockerClient(FakeFlockerClient())

    metric = get_metric(client=client, name=metric_name)
    measurement = get_measurement(
        clock=reactor, client=client, name=measurement_name,
    )
    scenario = get_scenario(
        clock=reactor, client=client, name=scenario_name,
    )
    version = client.version()

    d = gather_deferreds((metric, measurement, scenario, version))

    def got_parameters((metric, measurement, scenario, version)):
        d = sample(measurement, metric, scenario)
        d.addCallback(
            record_samples,
            version[u"flocker"],
            metric_name,
            measurement_name
        )
        return d

    d.addCallback(got_parameters)
    return d
