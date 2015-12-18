# Copyright 2015 ClusterHQ Inc.  See LICENSE file for details.
"""
Set up a Flocker cluster.
"""

import string
import sys
import yaml
from itertools import repeat
from pipes import quote as shell_quote

from eliot import add_destination, write_failure, FileDestination

from twisted.internet.defer import inlineCallbacks
from twisted.python.filepath import FilePath
from twisted.python.usage import UsageError

from .acceptance import (
    ClusterIdentity,
    CommonOptions,
    capture_journal,
    capture_upstart,
    eliot_output,
    get_trial_environment,
)

from flocker.apiclient import FlockerClient
from flocker.common import gather_deferreds, loop_until
from flocker.control.httpapi import REST_API_PORT


class RunOptions(CommonOptions):
    description = "Set up a Flocker cluster."

    optParameters = [
        ['image', None, None, 'Image to use for application containers'],
        ['apps-per-node', None, 0, 'Number of application containers per node',
         int],
        ['vols-per-node', None, 0, 'Number of data volumes per node',
         int],
        ['purpose', None, 'testing',
         "Purpose of the cluster recorded in its metadata where possible"],
    ]

    optFlags = [
        ["no-keep", None, "Do not keep VMs around (when testing)"],
    ]

    synopsis = ('Usage: cluster-setup --distribution <distribution> '
                '[--provider <provider>]')

    def __init__(self, top_level):
        """
        :param FilePath top_level: The top-level of the Flocker repository.
        """
        super(RunOptions, self).__init__(top_level)
        # Override default values defined in the base class.
        self['provider'] = self.defaults['provider'] = 'aws'
        self['dataset-backend'] = self.defaults['dataset-backend'] = 'aws'

    def postOptions(self):
        if self['app-template'] is not None:
            template_file = FilePath(self['app-template'])
            self['template'] = yaml.safe_load(template_file.getContent())
        if self['apps-per-node'] > 0 and self['image'] is None:
            raise UsageError(
                "image parameter must be provided if apps-per-node > 0"
            )

        self['purpose'] = unicode(self['purpose'])
        if any(x not in string.ascii_letters + string.digits + '-'
               for x in self['purpose']):
            raise UsageError(
                "Purpose may have only alphanumeric symbols and dash. " +
                "Found {!r}".format('purpose')
            )
        # This is run last as it creates the actual "runner" object
        # based on the provided parameters.
        super(RunOptions, self).postOptions()

    def _make_cluster_identity(self, dataset_backend):
        purpose = self['purpose']
        return ClusterIdentity(
            purpose=purpose,
            prefix=purpose,
            name='{}-cluster'.format(purpose).encode("ascii"),
        )


@inlineCallbacks
def main(reactor, args, base_path, top_level):
    """
    :param reactor: Reactor to use.
    :param list args: The arguments passed to the script.
    :param FilePath base_path: The executable being run.
    :param FilePath top_level: The top-level of the Flocker repository.
    """
    options = RunOptions(top_level=top_level)

    add_destination(eliot_output)
    try:
        options.parseOptions(args)
    except UsageError as e:
        sys.stderr.write("%s: %s\n" % (base_path.basename(), e))
        raise SystemExit(1)

    runner = options.runner

    from flocker.common.script import eliot_logging_service
    log_writer = eliot_logging_service(
        destination=FileDestination(
            file=open("%s.log" % (base_path.basename(),), "a")
        ),
        reactor=reactor,
        capture_stdout=False)
    log_writer.startService()
    reactor.addSystemEventTrigger(
        'before', 'shutdown', log_writer.stopService)

    cluster = None
    results = []
    try:
        yield runner.ensure_keys(reactor)
        cluster = yield runner.start_cluster(reactor)
        if options['distribution'] in ('centos-7',):
            remote_logs_file = open("remote_logs.log", "a")
            for node in cluster.all_nodes:
                results.append(capture_journal(reactor,
                                               node.address,
                                               remote_logs_file)
                               )
        elif options['distribution'] in ('ubuntu-14.04', 'ubuntu-15.10'):
            remote_logs_file = open("remote_logs.log", "a")
            for node in cluster.all_nodes:
                results.append(capture_upstart(reactor,
                                               node.address,
                                               remote_logs_file)
                               )
        gather_deferreds(results)

        client = _get_flocker_client(reactor, cluster)
        nodes = yield _get_nodes(reactor, client, cluster)

        results = []
        for node in nodes:
            for i in range(options['apps-per-node']):
                name = u"app_%s_%d" % (node.public_address, i)
                name = name.replace(u".", u"_")
                results.append(
                    client.create_container(node.uuid, name, options['image'])
                )
        yield gather_deferreds(results)

        print("Creaing volumes...")
        print("Please note that they are not automatically destroyed")
        results = []
        for node in nodes:
            for i in range(options['vols-per-node']):
                results.append(client.create_dataset(node.uuid))
        yield gather_deferreds(results)

        result = 0

    except BaseException:
        result = 1
        raise
    finally:
        if options['no-keep'] or result == 1:
            runner.stop_cluster(reactor)
        else:
            if cluster is None:
                print("Didn't finish creating the cluster.")
                runner.stop_cluster(reactor)
            else:
                print("The following variables describe the cluster:")
                environment_variables = get_trial_environment(cluster)
                for environment_variable in environment_variables:
                    print("export {name}={value};".format(
                        name=environment_variable,
                        value=shell_quote(
                            environment_variables[environment_variable]),
                    ))
                print("Be sure to preserve the required files.")

    raise SystemExit(result)


def _get_flocker_client(reactor, cluster):
    """
    Create a :class:`FlockerClient` object for accessing the given cluster.

    :param reactor: The reactor.
    :param flocker.provision._common.Cluster cluster: The target cluster.
    :return: The client object.
    :rtype: flocker.apiclient.FlockerClient
    """
    control_node = cluster.control_node.address
    certificates_path = cluster.certificates_path
    cluster_cert = certificates_path.child(b"cluster.crt")
    user_cert = certificates_path.child(b"user.crt")
    user_key = certificates_path.child(b"user.key")
    return FlockerClient(reactor, control_node, REST_API_PORT,
                         cluster_cert, user_cert, user_key)


def _get_nodes(reactor, client, cluster):
    """
    Wait until all nodes in the cluster are visible via the client API
    and then get the list of the nodes.

    :param reactor: The reactor.
    :param flocker.apiclient.FlockerClient client: The client connected to
        the cluster.
    :param flocker.provision._common.Cluster cluster: The target cluster.
    :return: ``Deferred`` firing with a ``list`` of
        :class:`flocker.apiclient.Node`.
    """
    def got_all_nodes():
        d = client.list_nodes()
        d.addCallback(
            lambda nodes: len(nodes) == len(cluster.agent_nodes)
        )
        d.addErrback(write_failure, logger=None)
        return d
    got_nodes = loop_until(reactor, got_all_nodes, repeat(1, 300))
    return got_nodes.addCallback(lambda _: client.list_nodes())
