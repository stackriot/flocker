# Copyright Hybrid Logic Ltd.  See LICENSE file for details.
"""
Run the acceptance tests.
"""

import sys
import os
import yaml
import json
from pipes import quote as shell_quote
from tempfile import mkdtemp

from zope.interface import Interface, implementer
from characteristic import attributes
from eliot import (
    add_destination, write_failure, FileDestination
)
from pyrsistent import pvector
from bitmath import GiB

from twisted.internet.error import ProcessTerminated
from twisted.python.usage import Options, UsageError
from twisted.python.filepath import FilePath
from twisted.internet.defer import inlineCallbacks, returnValue, succeed
from twisted.conch.ssh.keys import Key
from twisted.python.reflect import prefixedMethodNames

from effect import parallel
from txeffect import perform

from admin.vagrant import vagrant_version
from flocker.provision import PackageSource, Variants, CLOUD_PROVIDERS
from flocker.provision._ssh import (
    run_remotely,
    ensure_agent_has_ssh_key,
)
from flocker.provision._install import (
    ManagedNode,
    task_pull_docker_images,
    uninstall_flocker,
    install_flocker,
    configure_cluster,
    configure_zfs,
)
from flocker.provision._ca import Certificates
from flocker.provision._ssh._conch import make_dispatcher
from flocker.provision._common import Cluster
from flocker.acceptance.testtools import DatasetBackend
from flocker.testtools.cluster_utils import (
    make_cluster_id, Providers, TestTypes
)

from flocker.common.runner import run, run_ssh


def extend_environ(**kwargs):
    """
    Return a copy of ``os.environ`` with some additional environment variables
        added.

    :param **kwargs: The enviroment variables to add.
    :return dict: The new environment.
    """
    env = os.environ.copy()
    env.update(kwargs)
    return env


def remove_known_host(reactor, hostname):
    """
    Remove all keys belonging to hostname from a known_hosts file.

    :param reactor: Reactor to use.
    :param bytes hostname: Remove all keys belonging to this hostname from
        known_hosts.
    """
    return run(reactor, ['ssh-keygen', '-R', hostname])


def get_trial_environment(cluster):
    """
    Return a dictionary of environment varibles describing a cluster for
    accetpance testing.

    :param Cluster cluster: Description of the cluster to get environment
        variables for.
    """
    return {
        'FLOCKER_ACCEPTANCE_CONTROL_NODE': cluster.control_node.address,
        'FLOCKER_ACCEPTANCE_NUM_AGENT_NODES': str(len(cluster.agent_nodes)),
        'FLOCKER_ACCEPTANCE_VOLUME_BACKEND': cluster.dataset_backend.name,
        'FLOCKER_ACCEPTANCE_API_CERTIFICATES_PATH':
            cluster.certificates_path.path,
        'FLOCKER_ACCEPTANCE_HOSTNAME_TO_PUBLIC_ADDRESS': json.dumps({
            node.private_address: node.address
            for node in cluster.agent_nodes
            if node.private_address is not None
        }),
        'FLOCKER_ACCEPTANCE_DEFAULT_VOLUME_SIZE': bytes(
            cluster.default_volume_size
        ),
        'FLOCKER_ACCEPTANCE_TEST_VOLUME_BACKEND_CONFIG':
            cluster.dataset_backend_config_file.path
    }


def run_tests(reactor, cluster, trial_args):
    """
    Run the acceptance tests.

    :param Cluster cluster: The cluster to run acceptance tests against.
    :param list trial_args: Arguments to pass to trial. If not
        provided, defaults to ``['flocker.acceptance']``.

    :return int: The exit-code of trial.
    """
    if not trial_args:
        trial_args = ['--rterrors', 'flocker.acceptance']

    def check_result(f):
        f.trap(ProcessTerminated)
        if f.value.exitCode is not None:
            return f.value.exitCode
        else:
            return f

    return run(
        reactor,
        ['trial'] + list(trial_args),
        env=extend_environ(
            **get_trial_environment(cluster)
        )).addCallbacks(
            callback=lambda _: 0,
            errback=check_result,
            )


class IClusterRunner(Interface):
    """
    Interface for starting and stopping a cluster for acceptance testing.
    """

    def start_cluster(reactor):
        """
        Start cluster for running acceptance tests.

        :param reactor: Reactor to use.
        :return Deferred: Deferred which fires with a cluster to run
            tests against.
        """

    def stop_cluster(reactor):
        """
        Stop the cluster started by `start_cluster`.

        :param reactor: Reactor to use.
        :return Deferred: Deferred which fires when the cluster has been
            stopped.
        """

    def ensure_keys(reactor):
        """
        Ensure that the running ssh-agent has the ssh-keys needed to connect to
        created nodes.

        :param reactor: Reactor to use.
        :return Deferred: That fires with a succesful result if the key is
            found.  Otherwise, fails with ``AgentNotFound`` or ``KeyNotFound``.
        """


RUNNER_ATTRIBUTES = [
    # Name of the distribution the nodes run - eg "ubuntu-14.04"
    'distribution',

    'top_level', 'config', 'package_source', 'variants',

    # DatasetBackend named constant of the dataset backend the nodes use - eg
    # DatasetBackend.zfs
    'dataset_backend',

    # dict giving configuration for the dataset backend the nodes use - eg
    # {"pool": "flocker"}
    'dataset_backend_configuration',
]


@implementer(IClusterRunner)
class ManagedRunner(object):
    """
    An ``IClusterRunner`` implementation that doesn't start or stop nodes but
    only gives out access to nodes that are already running and managed by
    someone else.

    :ivar pvector _nodes: The ``ManagedNode`` instances representing the nodes
        that are already running that this object will pretend to start and
        stop.
    :ivar PackageSource package_source: The version of the software this object
        will install on the nodes when it "starts" them.
    :ivar NamedConstant dataset_backend: The ``DatasetBackend`` constant
        representing the dataset backend that the nodes will be configured to
        use when they are "started".
    :ivar dict dataset_backend_configuration: The backend-specific
        configuration the nodes will be given for their dataset backend.
    """
    def __init__(self, node_addresses, package_source, distribution,
                 dataset_backend, dataset_backend_configuration):
        """
        :param list: A ``list`` of public IP addresses or
            ``[private_address, public_address]`` lists.

        See ``ManagedRunner`` and ``ManagedNode`` for other parameter
        documentation.
        """
        # Blow up if the list contains mixed types.
        [address_type] = set(type(address) for address in node_addresses)
        if address_type is list:
            # A list of 2 item lists
            self._nodes = pvector(
                ManagedNode(
                    address=address,
                    private_address=private_address,
                    distribution=distribution
                )
                for (private_address, address) in node_addresses
            )
        else:
            # A list of strings.
            self._nodes = pvector(
                ManagedNode(address=address, distribution=distribution)
                for address in node_addresses
            )
        self.package_source = package_source
        self.dataset_backend = dataset_backend
        self.dataset_backend_configuration = dataset_backend_configuration

    def _upgrade_flocker(self, reactor, nodes, package_source):
        """
        Put the version of Flocker indicated by ``package_source`` onto all of
        the given nodes.

        This takes a primitive approach of uninstalling the software and then
        installing the new version instead of trying to take advantage of any
        OS-level package upgrade support.  Because it's easier.  The package
        removal step is allowed to fail in case the package is not installed
        yet (other failures are not differentiated).  The only action taken on
        failure is that the failure is logged.

        :param pvector nodes: The ``ManagedNode``\ s on which to upgrade the
            software.
        :param PackageSource package_source: The version of the software to
            which to upgrade.

        :return: A ``Deferred`` that fires when the software has been upgraded.
        """
        dispatcher = make_dispatcher(reactor)

        uninstalling = perform(dispatcher, uninstall_flocker(nodes))
        uninstalling.addErrback(write_failure, logger=None)

        def install(ignored):
            return perform(
                dispatcher,
                install_flocker(nodes, package_source),
            )
        installing = uninstalling.addCallback(install)
        return installing

    def ensure_keys(self, reactor):
        """
        Assume we have keys, since there's no way of asking the nodes what keys
        they'll accept.
        """
        return succeed(None)

    def start_cluster(self, reactor):
        """
        Don't start any nodes.  Give back the addresses of the configured,
        already-started nodes.
        """
        if self.package_source is not None:
            upgrading = self._upgrade_flocker(
                reactor, self._nodes, self.package_source
            )
        else:
            upgrading = succeed(None)

        def configure(ignored):
            return configured_cluster_for_nodes(
                reactor,
                generate_certificates(
                    make_cluster_id(
                        TestTypes.ACCEPTANCE,
                        _provider_for_cluster_id(self.dataset_backend),
                    ),
                    self._nodes),
                self._nodes,
                self.dataset_backend,
                self.dataset_backend_configuration,
                _save_backend_configuration(self.dataset_backend,
                                            self.dataset_backend_configuration)
            )
        configuring = upgrading.addCallback(configure)
        return configuring

    def stop_cluster(self, reactor):
        """
        Don't stop any nodes.
        """
        return succeed(None)


def _provider_for_cluster_id(dataset_backend):
    """
    Get the ``Providers`` value that probably corresponds to a value from
    ``DatasetBackend``.
    """
    if dataset_backend is DatasetBackend.aws:
        return Providers.AWS
    if dataset_backend is DatasetBackend.openstack:
        return Providers.OPENSTACK
    return Providers.UNSPECIFIED


def generate_certificates(cluster_id, nodes):
    """
    Generate a new set of certificates for the given nodes.

    :param UUID cluster_id: The unique identifier of the cluster for which to
        generate the certificates.
    :param list nodes: The ``INode`` providers that make up the cluster.

    :return: A ``Certificates`` instance referring to the newly generated
        certificates.
    """
    certificates_path = FilePath(mkdtemp())
    print("Generating certificates in: {}".format(certificates_path.path))
    certificates = Certificates.generate(
        certificates_path,
        nodes[0].address,
        len(nodes),
        cluster_id=cluster_id,
    )
    return certificates


def _save_backend_configuration(dataset_backend_name,
                                dataset_backend_configuration):
    """
    Saves the backend configuration to a local file for consumption by the
    trial process.

    :param dataset_backend_name: The name of the dataset_backend.

    :param dataset_backend_configuration: The configuration of the
        dataset_backend.

    :returns: The FilePath to the temporary file where the dataset backend
        configuration was saved.
    """
    dataset_path = FilePath(mkdtemp()).child('dataset-backend.yml')
    print("Saving dataset backend config to: {}".format(dataset_path.path))
    dataset_path.setContent(yaml.safe_dump(
            {dataset_backend_name.name: dataset_backend_configuration}))
    return dataset_path


def configured_cluster_for_nodes(
    reactor, certificates, nodes, dataset_backend,
    dataset_backend_configuration, dataset_backend_config_file
):
    """
    Get a ``Cluster`` with Flocker services running on the right nodes.

    :param reactor: The reactor.
    :param Certificates certificates: The certificates to install on the
        cluster.
    :param nodes: The ``ManagedNode``s on which to operate.
    :param NamedConstant dataset_backend: The ``DatasetBackend`` constant
        representing the dataset backend that the nodes will be configured to
        use when they are "started".
    :param dict dataset_backend_configuration: The backend-specific
        configuration the nodes will be given for their dataset backend.
    :param FilePath dataset_backend_config_file: A FilePath that has the
        dataset_backend info stored.

    :returns: A ``Deferred`` which fires with ``Cluster`` when it is
        configured.
    """
    # XXX: There is duplication between the values here and those in
    # f.node.agents.test.blockdevicefactory.MINIMUM_ALLOCATABLE_SIZES. We want
    # the default volume size to be greater than or equal to the minimum
    # allocatable size.
    #
    # Ideally, the minimum allocatable size (and perhaps the default volume
    # size) would be something known by an object that represents the dataset
    # backend. Unfortunately:
    #  1. There is no such object
    #  2. There is existing confusion in the code around 'openstack' and
    #     'rackspace'
    #
    # Here, we special-case Rackspace (presumably) because it has a minimum
    # allocatable size that is different from other Openstack backends.
    #
    # FLOC-2584 also discusses this.
    default_volume_size = GiB(1)
    if dataset_backend_configuration.get('auth_plugin') == 'rackspace':
        default_volume_size = GiB(100)

    cluster = Cluster(
        all_nodes=pvector(nodes),
        control_node=nodes[0],
        agent_nodes=nodes,
        dataset_backend=dataset_backend,
        default_volume_size=int(default_volume_size.to_Byte().value),
        certificates=certificates,
        dataset_backend_config_file=dataset_backend_config_file
    )

    configuring = perform(
        make_dispatcher(reactor),
        configure_cluster(cluster, dataset_backend_configuration)
    )
    configuring.addCallback(lambda ignored: cluster)
    return configuring


@implementer(IClusterRunner)
@attributes(RUNNER_ATTRIBUTES, apply_immutable=True)
class VagrantRunner(object):
    """
    Start and stop vagrant cluster for acceptance testing.

    :cvar list NODE_ADDRESSES: List of address of vagrant nodes created.
    """
    # TODO: This should acquire the vagrant image automatically,
    # rather than assuming it is available.
    # https://clusterhq.atlassian.net/browse/FLOC-1163

    NODE_ADDRESSES = ["172.16.255.250", "172.16.255.251"]

    def __init__(self):
        self.vagrant_path = self._get_vagrant_path(self.top_level,
                                                   self.distribution)

        self.certificates_path = self.top_level.descendant([
            'vagrant', 'tutorial', 'credentials'])

        if self.variants:
            raise UsageError("Variants unsupported on vagrant.")

    def _get_vagrant_path(self, top_level, distribution):
        """
        Get the path to the Vagrant directory for ``distribution``.

        :param FilePath top_level: the directory containing the ``admin``
            package.
        :param bytes distribution: the name of a distribution
        :raise UsageError: if no such distribution found.
        :return: ``FilePath`` of the vagrant directory.
        """
        vagrant_dir = top_level.descendant([
            'admin', 'vagrant-acceptance-targets'
        ])
        vagrant_path = vagrant_dir.child(distribution)
        if not vagrant_path.exists():
            distributions = vagrant_dir.listdir()
            raise UsageError(
                "Distribution not found: %s. Valid distributions: %s."
                % (self.distribution, ', '.join(distributions)))
        return vagrant_path

    def ensure_keys(self, reactor):
        key = Key.fromFile(os.path.expanduser(
            "~/.vagrant.d/insecure_private_key"))
        return ensure_agent_has_ssh_key(reactor, key)

    @inlineCallbacks
    def start_cluster(self, reactor):
        # Destroy the box to begin, so that we are guaranteed
        # a clean build.
        yield run(
            reactor,
            ['vagrant', 'destroy', '-f'],
            path=self.vagrant_path.path)

        if self.package_source.version:
            env = extend_environ(
                FLOCKER_BOX_VERSION=vagrant_version(
                    self.package_source.version))
        else:
            env = os.environ
        # Boot the VMs
        yield run(
            reactor,
            ['vagrant', 'up'],
            path=self.vagrant_path.path,
            env=env)

        for node in self.NODE_ADDRESSES:
            yield remove_known_host(reactor, node)

        nodes = pvector(
            ManagedNode(address=address, distribution=self.distribution)
            for address in self.NODE_ADDRESSES
        )

        certificates = Certificates(self.certificates_path)
        # Default volume size is meaningless here as Vagrant only uses ZFS, and
        # not a block device backend.
        # XXX Change ``Cluster`` to not require default_volume_size
        default_volume_size = int(GiB(1).to_Byte().value)
        cluster = Cluster(
            all_nodes=pvector(nodes),
            control_node=nodes[0],
            agent_nodes=nodes,
            dataset_backend=self.dataset_backend,
            certificates=certificates,
            default_volume_size=default_volume_size,
        )

        returnValue(cluster)

    def stop_cluster(self, reactor):
        return run(
            reactor,
            ['vagrant', 'destroy', '-f'],
            path=self.vagrant_path.path)


@attributes(RUNNER_ATTRIBUTES + [
    'provisioner', 'num_nodes',
], apply_immutable=True)
class LibcloudRunner(object):
    """
    Start and stop cloud cluster for acceptance testing.

    :ivar LibcloudProvioner provisioner: The provisioner to use to create the
        nodes.
    :ivar DatasetBackend dataset_backend: The volume backend the nodes are
        configured with.
    """

    def __init__(self):
        self.nodes = []

        self.metadata = self.config.get('metadata', {})
        try:
            creator = self.metadata['creator']
        except KeyError:
            raise UsageError("Must specify creator metadata.")

        if not creator.isalnum():
            raise UsageError(
                "Creator must be alphanumeric. Found {!r}".format(creator)
            )
        self.creator = creator

    @inlineCallbacks
    def start_cluster(self, reactor):
        """
        Provision cloud cluster for acceptance tests.

        :return Cluster: The cluster to connect to for acceptance tests.
        """
        metadata = {
            'purpose': 'acceptance-testing',
            'distribution': self.distribution,
        }
        metadata.update(self.metadata)

        for index in range(self.num_nodes):
            name = "acceptance-test-%s-%d" % (self.creator, index)
            try:
                print "Creating node %d: %s" % (index, name)
                node = self.provisioner.create_node(
                    name=name,
                    distribution=self.distribution,
                    metadata=metadata,
                )
            except:
                print "Error creating node %d: %s" % (index, name)
                print "It may have leaked into the cloud."
                raise

            yield remove_known_host(reactor, node.address)
            self.nodes.append(node)
            del node

        commands = parallel([
            node.provision(package_source=self.package_source,
                           variants=self.variants)
            for node in self.nodes
        ])
        if self.dataset_backend == DatasetBackend.zfs:
            zfs_commands = parallel([
                configure_zfs(node, variants=self.variants)
                for node in self.nodes
            ])
            commands = commands.on(success=lambda _: zfs_commands)

        yield perform(make_dispatcher(reactor), commands)

        cluster = yield configured_cluster_for_nodes(
            reactor,
            generate_certificates(
                make_cluster_id(
                    TestTypes.ACCEPTANCE,
                    _provider_for_cluster_id(self.dataset_backend),
                ),
                self.nodes),
            self.nodes,
            self.dataset_backend,
            self.dataset_backend_configuration,
            _save_backend_configuration(self.dataset_backend,
                                        self.dataset_backend_configuration)
        )

        returnValue(cluster)

    def stop_cluster(self, reactor):
        """
        Deprovision the cluster provisioned by ``start_cluster``.
        """
        for node in self.nodes:
            try:
                print "Destroying %s" % (node.name,)
                node.destroy()
            except Exception as e:
                print "Failed to destroy %s: %s" % (node.name, e)

    def ensure_keys(self, reactor):
        key = self.provisioner.get_ssh_key()
        if key is not None:
            return ensure_agent_has_ssh_key(reactor, key)
        else:
            return succeed(None)


DISTRIBUTIONS = ('centos-7', 'ubuntu-14.04')


class RunOptions(Options):
    description = "Run the acceptance tests."

    optParameters = [
        ['distribution', None, None,
         'The target distribution. '
         'One of {}.'.format(', '.join(DISTRIBUTIONS))],
        ['provider', None, 'vagrant',
         'The compute-resource provider to test against. '
         'One of {}.'],
        ['dataset-backend', None, 'zfs',
         'The dataset backend to test against. '
         'One of {}'.format(', '.join(backend.name for backend
                                      in DatasetBackend.iterconstants()))],
        ['config-file', None, None,
         'Configuration for compute-resource providers and dataset backends.'],
        ['branch', None, None, 'Branch to grab packages from'],
        ['flocker-version', None, None, 'Version of flocker to install'],
        ['build-server', None, 'http://build.clusterhq.com/',
         'Base URL of build server for package downloads'],
    ]

    optFlags = [
        ["keep", "k", "Keep VMs around, if the tests fail."],
        ["no-pull", None,
         "Do not pull any Docker images when provisioning nodes."],
    ]

    synopsis = ('Usage: run-acceptance-tests --distribution <distribution> '
                '[--provider <provider>] [<test-cases>]')

    def __init__(self, top_level):
        """
        :param FilePath top_level: The top-level of the flocker repository.
        """
        Options.__init__(self)
        self.docs['provider'] = self.docs['provider'].format(
            self._get_provider_names()
        )
        self.top_level = top_level
        self['variants'] = []

    def _get_provider_names(self):
        """
        Find the names of all supported "providers" (eg Vagrant, Rackspace).

        :return: A ``list`` of ``str`` giving all such names.
        """
        return prefixedMethodNames(self.__class__, "_runner_")

    def opt_variant(self, arg):
        """
        Specify a variant of the provisioning to run.

        Supported variants: distro-testing, docker-head, zfs-testing.
        """
        self['variants'].append(Variants.lookupByValue(arg))

    def parseArgs(self, *trial_args):
        self['trial-args'] = trial_args

    def dataset_backend_configuration(self):
        """
        Get the configuration corresponding to storage driver chosen by the
        command line options.
        """
        drivers = self['config'].get('storage-drivers', {})
        configuration = drivers.get(self['dataset-backend'], {})
        return configuration

    def dataset_backend(self):
        """
        Get the storage driver the acceptance testing nodes will use.

        :return: A constant from ``DatasetBackend`` matching the name of the
            backend chosen by the command-line options.
        """
        configuration = self.dataset_backend_configuration()
        # Avoid requiring repetition of the backend name when it is the same as
        # the name of the configuration section.  But allow it so that there
        # can be "great-openstack-provider" and "better-openstack-provider"
        # sections side-by-side that both use "openstack" backend but configure
        # it slightly differently.
        dataset_backend_name = configuration.get(
            "backend", self["dataset-backend"]
        )
        try:
            return DatasetBackend.lookupByName(dataset_backend_name)
        except ValueError:
            raise UsageError(
                "Unknown dataset backend: {}".format(
                    dataset_backend_name
                )
            )

    def postOptions(self):
        if self['distribution'] is None:
            raise UsageError("Distribution required.")

        if self['config-file'] is not None:
            config_file = FilePath(self['config-file'])
            self['config'] = yaml.safe_load(config_file.getContent())
        else:
            self['config'] = {}

        provider = self['provider'].lower()
        provider_config = self['config'].get(provider, {})

        package_source = PackageSource(
            version=self['flocker-version'],
            branch=self['branch'],
            build_server=self['build-server'],
        )
        try:
            get_runner = getattr(self, "_runner_" + provider.upper())
        except AttributeError:
            raise UsageError(
                "Provider {!r} not supported. Available providers: {}".format(
                    provider, ', '.join(
                        name.lower() for name in self._get_provider_names()
                    )
                )
            )
        else:
            self.runner = get_runner(
                package_source=package_source,
                dataset_backend=self.dataset_backend(),
                provider_config=provider_config,
            )

    def _provider_config_missing(self, provider):
        """
        :param str provider: The name of the missing provider.
        :raise: ``UsageError`` indicating which provider configuration was
                missing.
        """
        raise UsageError(
            "Configuration file must include a "
            "{!r} config stanza.".format(provider)
        )

    def _runner_VAGRANT(self, package_source,
                        dataset_backend, provider_config):
        """
        :param PackageSource package_source: The source of omnibus packages.
        :param DatasetBackend dataset_backend: A ``DatasetBackend`` constant.
        :param provider_config: The ``vagrant`` section of the acceptance
            testing configuration file.  Since the Vagrant runner accepts no
            configuration, this is ignored.
        :returns: ``VagrantRunner``
        """
        return VagrantRunner(
            config=self['config'],
            top_level=self.top_level,
            distribution=self['distribution'],
            package_source=package_source,
            variants=self['variants'],
            dataset_backend=dataset_backend,
            dataset_backend_configuration=self.dataset_backend_configuration()
        )

    def _runner_MANAGED(self, package_source, dataset_backend,
                        provider_config):
        """
        :param PackageSource package_source: The source of omnibus packages.
        :param DatasetBackend dataset_backend: A ``DatasetBackend`` constant.
        :param provider_config: The ``managed`` section of the acceptance
            testing configuration file.  The section of the configuration
            file should look something like:

                managed:
                  addresses:
                    - "172.16.255.240"
                    - "172.16.255.241"
                  distribution: "centos-7"
        :returns: ``ManagedRunner``.
        """
        if provider_config is None:
            self._provider_config_missing("managed")

        if not provider_config.get("upgrade"):
            package_source = None

        return ManagedRunner(
            node_addresses=provider_config['addresses'],
            package_source=package_source,
            # TODO LATER Might be nice if this were part of
            # provider_config. See FLOC-2078.
            distribution=self['distribution'],
            dataset_backend=dataset_backend,
            dataset_backend_configuration=self.dataset_backend_configuration(),
        )

    def _libcloud_runner(self, package_source, dataset_backend,
                         provider, provider_config):
        """
        Run some nodes using ``libcloud``.

        By default, two nodes are run.  This can be overridden by setting
        ``FLOCKER_ACCEPTANCE_NUM_NODES`` in the environment.

        :param PackageSource package_source: The source of omnibus packages.
        :param DatasetBackend dataset_backend: A ``DatasetBackend`` constant.
        :param provider: The name of the cloud provider of nodes for the tests.
        :param provider_config: The ``managed`` section of the acceptance

        :returns: ``LibcloudRunner``.
        """
        if provider_config is None:
            self._provider_config_missing(provider)

        provisioner = CLOUD_PROVIDERS[provider](**provider_config)
        return LibcloudRunner(
            config=self['config'],
            top_level=self.top_level,
            distribution=self['distribution'],
            package_source=package_source,
            provisioner=provisioner,
            dataset_backend=dataset_backend,
            dataset_backend_configuration=self.dataset_backend_configuration(),
            variants=self['variants'],
            num_nodes=int(os.environ.get("FLOCKER_ACCEPTANCE_NUM_NODES", "2")),
        )

    def _runner_RACKSPACE(self, package_source, dataset_backend,
                          provider_config):
        """
        :param PackageSource package_source: The source of omnibus packages.
        :param DatasetBackend dataset_backend: A ``DatasetBackend`` constant.
        :param provider_config: The ``rackspace`` section of the acceptance
            testing configuration file.  The section of the configuration
            file should look something like:

               rackspace:
                 region: <rackspace region, e.g. "iad">
                 username: <rackspace username>
                 key: <access key>
                 keyname: <ssh-key-name>

        :see: :ref:`acceptance-testing-rackspace-config`
        """
        return self._libcloud_runner(
            package_source, dataset_backend, "rackspace", provider_config
        )

    def _runner_AWS(self, package_source, dataset_backend,
                    provider_config):
        """
        :param PackageSource package_source: The source of omnibus packages.
        :param DatasetBackend dataset_backend: A ``DatasetBackend`` constant.
        :param provider_config: The ``aws`` section of the acceptance testing
            configuration file.  The section of the configuration file should
            look something like:

               aws:
                 region: <aws region, e.g. "us-west-2">
                 zone: <aws zone, e.g. "us-west-2a">
                 access_key: <aws access key>
                 secret_access_token: <aws secret access token>
                 keyname: <ssh-key-name>
                 security_groups: ["<permissive security group>"]

        :see: :ref:`acceptance-testing-aws-config`
        """
        return self._libcloud_runner(
            package_source, dataset_backend, "aws", provider_config
        )

MESSAGE_FORMATS = {
    "flocker.provision.ssh:run":
        "[%(username)s@%(address)s]: Running %(command)s\n",
    "flocker.provision.ssh:run:output":
        "[%(username)s@%(address)s]: %(line)s\n",
    "flocker.common.runner:run:stdout":
        "%(line)s\n",
    "flocker.common.runner:run:stderr":
        "stderr:%(line)s\n",
}
ACTION_START_FORMATS = {
    "flocker.common.runner:run":
        "Running %(command)s\n",
}


def eliot_output(message):
    """
    Write pretty versions of eliot log messages to stdout.
    """
    message_type = message.get('message_type')
    action_type = message.get('action_type')
    action_status = message.get('action_status')

    format = ''
    if message_type is not None:
        if message_type == 'twisted:log' and message.get('error'):
            format = '%(message)s'
        else:
            format = MESSAGE_FORMATS.get(message_type, '')
    elif action_type is not None:
        if action_status == 'started':
            format = ACTION_START_FORMATS.get('action_type', '')
        # We don't consider other status, since we
        # have no meaningful messages to write.
    sys.stdout.write(format % message)
    sys.stdout.flush()


def capture_upstart(reactor, host, output_file):
    """
    SSH into given machine and capture relevant logs, writing them to
    output file.

    :param reactor: The reactor.
    :param bytes host: Machine to SSH into.
    :param file output_file: File to write to.
    """
    formatter = upstart_json_formatter(output_file)

    ran = run_ssh(
        reactor=reactor,
        host=host,
        username='root',
        command=[
            b'tail -f /var/log/flocker/*.log /var/log/upstart/docker.log',
        ],
        handle_stdout=formatter,
    )
    ran.addErrback(write_failure, logger=None)
    # Deliver a final empty line to process the last message
    ran.addCallback(lambda ignored: formatter(b""))

def upstart_json_formatter(output_file):
    """
    Create an output handler which turns journald's export format back into
    Eliot JSON with extra fields to identify the log origin.
    """
    def handle_output_line(line):
        output_file.write(json.dumps(line) + b"\n")
    return handle_output_line

def capture_journal(reactor, host, output_file):
    """
    SSH into given machine and capture relevant logs, writing them to
    output file.

    :param reactor: The reactor.
    :param bytes host: Machine to SSH into.
    :param file output_file: File to write to.
    """
    formatter = journald_json_formatter(output_file)
    ran = run_ssh(
        reactor=reactor,
        host=host,
        username='root',
        command=[
            b'journalctl',
            b'--lines', b'0',
            b'--output', b'export',
            b'--follow',
            # Only bother with units we care about:
            b'-u', b'docker',
            b'-u', b'flocker-control',
            b'-u', b'flocker-dataset-agent',
            b'-u', b'flocker-container-agent',
            b'-u', b'flocker-docker-plugin',
        ],
        handle_stdout=formatter,
    )
    ran.addErrback(write_failure, logger=None)
    # Deliver a final empty line to process the last message
    ran.addCallback(lambda ignored: formatter(b""))


def journald_json_formatter(output_file):
    """
    Create an output handler which turns journald's export format back into
    Eliot JSON with extra fields to identify the log origin.
    """
    accumulated = {}

    # XXX Factoring the parsing code separately from the IO would make this
    # whole thing nicer.
    def handle_output_line(line):
        if line:
            key, value = line.split(b"=", 1)
            accumulated[key] = value
        else:
            if accumulated:
                raw_message = accumulated.get(b"MESSAGE", b"{}")
                try:
                    message = json.loads(raw_message)
                except ValueError:
                    # Docker log messages are not JSON
                    message = dict(message=raw_message)

                message[u"_HOSTNAME"] = accumulated.get(
                    b"_HOSTNAME", b"<no hostname>"
                )
                message[u"_SYSTEMD_UNIT"] = accumulated.get(
                    b"_SYSTEMD_UNIT", b"<no unit>"
                )
                output_file.write(json.dumps(message) + b"\n")
                accumulated.clear()
    return handle_output_line


@inlineCallbacks
def main(reactor, args, base_path, top_level):
    """
    :param reactor: Reactor to use.
    :param list args: The arguments passed to the script.
    :param FilePath base_path: The executable being run.
    :param FilePath top_level: The top-level of the flocker repository.
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
    try:
        yield runner.ensure_keys(reactor)
        cluster = yield runner.start_cluster(reactor)

        if options['distribution'] in ('centos-7',):
            remote_logs_file = open("remote_logs.log", "a")
            for node in cluster.all_nodes:
                capture_journal(reactor, node.address, remote_logs_file)
        elif options['distribution'] in ('ubuntu-14.04', 'ubuntu-15.04',):
            remote_logs_file = open("remote_logs.log", "a")
            for node in cluster.all_nodes:
                capture_upstart(reactor, node.address, remote_logs_file)

        if not options["no-pull"]:
            yield perform(
                make_dispatcher(reactor),
                parallel([
                    run_remotely(
                        username='root',
                        address=node.address,
                        commands=task_pull_docker_images()
                    ) for node in cluster.agent_nodes
                ]),
            )

        result = yield run_tests(
            reactor=reactor,
            cluster=cluster,
            trial_args=options['trial-args'])
    except:
        result = 1
        raise
    finally:
        # Unless the tests failed, and the user asked to keep the nodes, we
        # delete them.
        if not options['keep']:
            runner.stop_cluster(reactor)
        else:
            print "--keep specified, not destroying nodes."
            if cluster is None:
                print ("Didn't finish creating the cluster.")
            else:
                print ("To run acceptance tests against these nodes, "
                       "set the following environment variables: ")

                environment_variables = get_trial_environment(cluster)

                for environment_variable in environment_variables:
                    print "export {name}={value};".format(
                        name=environment_variable,
                        value=shell_quote(
                            environment_variables[environment_variable]),
                    )

    raise SystemExit(result)
