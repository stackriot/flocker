# Copyright ClusterHQ Inc.  See LICENSE file for details.

"""
Tests for the Flocker Docker plugin.
"""

from twisted.internet import reactor
from twisted.trial.unittest import SkipTest

from distutils.version import LooseVersion

from ...common import loop_until
from ...common.runner import run_ssh

from ...testtools import (
    AsyncTestCase, random_name, find_free_port, flaky,
)
from ..testtools import (
    require_cluster, post_http_server, assert_http_server,
    get_docker_client, verify_socket, check_http_server, DatasetBackend,
    create_dataset,
)

from ..scripts import SCRIPTS

from ...node.agents.ebs import EBSMandatoryProfileAttributes


class DockerPluginTests(AsyncTestCase):
    """
    Tests for the Docker plugin.
    """
    def require_docker(self, required_version, cluster):
        """
        Check for a specific minimum version of Docker on a remote node.
        """
        client = get_docker_client(cluster, cluster.nodes[0].public_address)
        client_version = LooseVersion(client.version()['Version'])
        minimum_version = LooseVersion(required_version)
        if client_version < minimum_version:
            raise SkipTest(
                'This test requires at least Docker {}. '
                'Actual version: {}'.format(minimum_version, client_version)
            )

    def docker_service(self, address, action):
        """
        Restart the Docker daemon on the specified address.
        Restarts by stopping, verifying the service has stopped, then
        starting and finally verifying the service has started.
        Verification is performed via the success of running "docker ps"
        on the target node; an exit code of 0 indicates the daemon is
        running, an exit code of 1 is expected if we try to run this
        command when the daemon is stopped.

        :param bytes address: The public IP of the node on which Docker will
            be restarted.
        :param bytes action: The action to perform on the service.
        """
        distro = []
        get_distro = run_ssh(
            reactor, b"root", address, ["python", "-m", "platform"],
            handle_stdout=distro.append)
        get_distro.addCallback(lambda _: distro[0].lower())

        def action_docker(distribution):
            if 'ubuntu' in distribution:
                command = ["service", "docker", action]
            else:
                command = ["systemctl", action, "docker"]
            d = run_ssh(reactor, b"root", address, command)

            def handle_error(_, action):
                self.fail(
                    "Docker {} failed. See logs for process output.".format(
                        action))

            d.addErrback(handle_error, action)
            return d

        acting = get_distro.addCallback(action_docker)
        return acting

    def run_python_container(self, cluster, address, docker_arguments, script,
                             script_arguments, cleanup=True, client=None):
        """
        Run a Python script as a Docker container with the Flocker volume
        driver.

        This is a blocking call.

        :param Cluster cluster: Description of the cluster we're talking to.
        :param bytes address: The public IP of the node where it will run.
        :param dict docker_arguments: Additional arguments to pass to
            Docker ``create_container()`` call.
        :param FilePath script: The script to run.
        :param list script_arguments: Additional arguments to pass to the
            script.
        :param cleanup: If true, cleanup the container at test end.

        :return: Container id, once the Docker container has started.
        """
        if client is None:
            client = get_docker_client(cluster, address)

        # Remove all existing containers on the node, in case they're left
        # over from previous test:
        for container in client.containers():
            client.remove_container(container["Id"], force=True)

        container = client.create_container(
            "python:2.7-slim",
            ["python", "-c", script.getContent()] + list(script_arguments),
            volume_driver="flocker", **docker_arguments)
        cid = container["Id"]
        client.start(container=cid)
        if cleanup:
            self.addCleanup(client.remove_container, cid, force=True)
        return cid

    def _create_volume(self, client, name, driver_opts):
        """
        Create a volume with the given name and driver options on the passed in
        docker client.

        :param client: The docker.Client object to use.
        :param name: The name of the volume to create.
        :param opts: The options to pass through to the Flocker Plugin for
            Docker.
        :returns: The result of the API call.
        """
        # XXX: replace down to addCleanup with:
        #
        # result = client.create_volume(name, u'flocker', driver_opts)
        #
        # Once version 1.5.1+ of docker-py is released. It is currently fixed
        # at head, but version 1.5.0 unfortunately has the wrong endpoint for
        # creating a volume.
        url = client._url('/volumes/create')
        data = {
            'Name': name,
            'Driver': u'flocker',
            'DriverOpts': driver_opts,
        }
        result = client._result(client._post_json(url, data=data), True)
        self.addCleanup(client.remove_volume, name)
        return result

    def _test_create_container(self, cluster, volume_name=None):
        """
        Create a container running a simple HTTP server that writes to
        its volume on POST and reads the same data back from the volume
        on GET.

        :param cluster: The cluster to create the container within.
        :param unicode volume_name: The name to use for the volume. If left
            unspecified then a random name will be generated.
        """
        data = random_name(self).encode("utf-8")
        node = cluster.nodes[0]
        client = get_docker_client(cluster, node.public_address)
        http_port = 8080
        host_port = find_free_port()[1]

        if volume_name is None:
            volume_name = random_name(self)
        self.run_python_container(
            cluster, node.public_address,
            {"host_config": client.create_host_config(
                binds=["{}:/data".format(volume_name)],
                port_bindings={http_port: host_port},
                restart_policy={"Name": "always"}),
             "ports": [http_port]},
            SCRIPTS.child(b"datahttp.py"),
            # This tells the script where it should store its data,
            # and we want it to specifically use the volume:
            [u"/data"], client=client)

        d = post_http_server(self, node.public_address, host_port,
                             {"data": data})
        d.addCallback(lambda _: assert_http_server(
            self, node.public_address, host_port, expected_response=data))
        return d

    @flaky(u'FLOC-3346')
    @require_cluster(1)
    def test_create_container_with_v2_plugin_api(self, cluster):
        """
        Docker >=1.9, using the v2 plugin API, can run a container
        with a volume provisioned by Flocker.
        """
        self.require_docker('1.9.0', cluster)
        return self._test_create_container(cluster)

    @require_cluster(1, required_backend=DatasetBackend.aws)
    def test_create_silver_volume_with_v2_plugin_api(self, cluster, backend):
        """
        Docker >=1.9, using the v2 plugin API, can create a volume with the
        volumes API, and pass a profile argument of silver which is represented
        in the backing volume created on EBS.

        The approach here is to:

        - Create the volume.
        - Create a container using that volume to ensure that it is created.
        - Use the flocker API to find the dataset for the volume.
        - Interface with EBS directly to verify that the created volume has
          'silver' characteristics.
        """
        self.require_docker('1.9.0', cluster)
        node = cluster.nodes[0]
        docker = get_docker_client(cluster, node.public_address)
        volume_name = random_name(self)
        self._create_volume(docker, volume_name,
                            driver_opts={'profile': 'silver'})

        create_container = self._test_create_container(cluster,
                                                       volume_name=volume_name)

        def _get_datasets(unused_arg):
            return cluster.client.list_datasets_configuration()
        create_container.addCallback(_get_datasets)

        def _verify_created_volume_is_silver(datasets):
            dataset = next(d for d in datasets
                           if d.metadata.get(u'name') == volume_name)

            volumes = backend.list_volumes()

            volume = next(v for v in volumes
                          if v.dataset_id == dataset.dataset_id)
            ebs_volume = backend._get_ebs_volume(volume.blockdevice_id)

            self.assertEqual(
                EBSMandatoryProfileAttributes.SILVER.value.volume_type.value,
                ebs_volume.type)

        create_container.addCallback(_verify_created_volume_is_silver)

        return create_container

    @require_cluster(1)
    def test_volume_persists_restart(self, cluster):
        """
        If a container with a volume is created with a restart policy of
        "always", the container will restart with the same volume attached
        after the Docker daemon is restarted.
        """
        # create a simple data HTTP python container, with the restart policy
        data = random_name(self).encode("utf-8")
        node = cluster.nodes[0]
        client = get_docker_client(cluster, node.public_address)

        http_port = 8080
        host_port = find_free_port()[1]

        volume_name = random_name(self)
        self.run_python_container(
            cluster, node.public_address,
            {"host_config": client.create_host_config(
                binds=["{}:/data".format(volume_name)],
                port_bindings={http_port: host_port},
                restart_policy={"Name": "always"}),
             "ports": [http_port]},
            SCRIPTS.child(b"datahttp.py"),
            # This tells the script where it should store its data,
            # and we want it to specifically use the volume:
            [u"/data"], client=client)

        # write some data to it via POST
        d = post_http_server(self, node.public_address, host_port,
                             {"data": data})
        # assert the data has been written
        d.addCallback(lambda _: assert_http_server(
            self, node.public_address, host_port, expected_response=data))
        # stop the Docker daemon
        d.addCallback(lambda _: self.docker_service(
            node.public_address, b"stop"))

        # ensure the container HTTP service has stopped

        def poll_http_server_stopped(_):
            def http_closed():
                ds = check_http_server(node.public_address, host_port)
                ds.addCallback(lambda succeeded: not succeeded)
                return ds
            looping = loop_until(reactor, http_closed)
            return looping

        d.addCallback(poll_http_server_stopped)
        # start Docker daemon
        d.addCallback(lambda _: self.docker_service(
            node.public_address, b"start"))
        # attempt to read the data back again; the container should've
        # restarted automatically, though it may take a few seconds
        # after the Docker daemon has restarted.

        def poll_http_server(_):
            ds = verify_socket(node.public_address, host_port)
            ds.addCallback(lambda _: assert_http_server(
                self, node.public_address, host_port, expected_response=data)
            )
            return ds

        d.addCallback(poll_http_server)
        return d

    @require_cluster(1)
    def test_run_container_with_volume(self, cluster):
        """
        Docker can run a container with a volume provisioned by Flocker.
        """
        return self._test_create_container(cluster)

    @require_cluster(1)
    def test_run_container_with_preexisting_volume(self, cluster):
        """
        Docker can run a container with a volume provisioned by Flocker before
        it is mentioned to the Docker plugin.
        """
        name = random_name(self)
        d = create_dataset(self, cluster, metadata={u"name": name})
        d.addCallback(lambda _: self._test_create_container(
            cluster, volume_name=name))
        return d

    def _test_move(self, cluster, origin_node, destination_node):
        """
        Assert that Docker can run a container with a volume provisioned by
        Flocker, shut down the container and then start a new container
        with the same volume on the specified node.

        :param cluster: The ``Cluster`` to talk to.
        :param Node origin_node: Original node to start container on.
        :param Node destination_node: Original node to start container on.

        :return: ``Deferred`` that fires on assertion success, or failure.
        """
        client = get_docker_client(cluster, origin_node.public_address)
        data = "hello world"
        http_port = 8080
        host_port = find_free_port()[1]
        volume_name = random_name(self)
        container_args = {
            "host_config": client.create_host_config(
                binds=["{}:/data".format(volume_name)],
                port_bindings={http_port: host_port}),
            "ports": [http_port]}

        cid = self.run_python_container(
            cluster, origin_node.public_address, container_args,
            SCRIPTS.child(b"datahttp.py"),
            # This tells the script where it should store its data,
            # and we want it to specifically use the volume:
            [u"/data"], cleanup=False, client=client)

        # Post to container on origin node:
        d = post_http_server(self, origin_node.public_address, host_port,
                             {"data": data})

        def posted(_):
            # Shutdown original container:
            client = get_docker_client(cluster, origin_node.public_address)
            client.remove_container(cid, force=True)
            # Start container on destination node with same volume:
            self.run_python_container(
                cluster, destination_node.public_address, container_args,
                SCRIPTS.child(b"datahttp.py"), [u"/data"])
        d.addCallback(posted)
        d.addCallback(lambda _: assert_http_server(
            self, destination_node.public_address, host_port,
            expected_response=data))
        return d

    @flaky(u'FLOC-2977')
    @require_cluster(1)
    def test_move_volume_single_node(self, cluster):
        """
        Docker can run a container with a volume provisioned by Flocker, shut
        down the container and then start a new container with the same
        volume on the same machine.
        """
        return self._test_move(cluster, cluster.nodes[0], cluster.nodes[0])

    @flaky(u'FLOC-3346')
    @require_cluster(2)
    def test_move_volume_different_node(self, cluster):
        """
        Docker can run a container with a volume provisioned by Flocker, shut
        down the container and then start a new container with the same
        volume on a different machine.
        """
        return self._test_move(cluster, cluster.nodes[0], cluster.nodes[1])
