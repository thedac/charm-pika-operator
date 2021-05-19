#!/usr/bin/env python3
# Copyright 2021 David
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

    https://discourse.charmhub.io/t/4208
"""

import logging

from charms.interface_rabbitmq_amqp.v0.rabbitmq import RabbitMQAMQPRequires
from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus

logger = logging.getLogger(__name__)


class PikaOperatorCharm(CharmBase):
    """Charm the service."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.pika_pebble_ready, self._on_pika_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self._stored.set_default(password=None)
        # AMQP Requires
        self.amqp_requires = (RabbitMQAMQPRequires(self, "amqp"))
        self.framework.observe(self.amqp_requires.on.has_amqp_servers, self._on_has_amqp_servers)
        self.framework.observe(
            self.amqp_requires.on.ready_amqp_servers, self._on_ready_amqp_servers)
        self.framework.observe(self.on.query_amqp_action, self._on_query_amqp)

    def _on_pika_pebble_ready(self, event):
        # Get a reference the container attribute on the PebbleReadyEvent
        container = event.workload
        # Define an initial Pebble layer configuration
        pebble_layer = {
            "summary": " layer",
            "description": "pebble config layer for pika",
            "services": {}
        }
        # Add intial Pebble config layer using the Pebble API
        container.add_layer("", pebble_layer, combine=True)
        # Autostart any services that were defined with startup: enabled
        # container.autostart()
        # Learn more about statuses in the SDK docs:
        # https://juju.is/docs/sdk/constructs#heading--statuses
        self.unit.status = ActiveStatus()

    @property
    def queue(self):
        return "pika-queue"

    @property
    def vhost(self):
        return "pika-vhost"

    @property
    def username(self):
        return "pika"

    @property
    def password(self):
        return self._stored.password

    def _on_has_amqp_servers(self, event):
        logging.info("Requesting user and vhost")
        self.amqp_requires.request_access(
            self.amqp_requires.username, self.amqp_requires.vhost)
        self.unit.status = ActiveStatus()

    def _on_ready_amqp_servers(self, event):
        logging.info("Setting password")
        self._stored.password = event.password

    def _on_config_changed(self, _):
        pass

    def _on_query_amqp(self, event):
        import pika

        try:
            logging.info("Attempting to query AMQP")
            credentials = credentials = pika.PlainCredentials(self.username, self.password)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost', 5672, self.vhost, credentials))
            channel = connection.channel()

            # Purge queue
            # http://pika.readthedocs.org/en/latest/modules/channel.html?highlight=purge#pika.channel.Channel.queue_purge
            channel.queue_declare(queue=self.queue)

            channel.basic_publish(exchange='', routing_key=self.vhost, body='Hello World!')
            logging.info("Sent 'Hello World!'")

            def callback(ch, method, properties, body):
                logging.info(" [x] Received %r" % body)
                event.set_results({"result": "found '{}'".format(body)})

            channel.basic_consume(queue=self.vhost, auto_ack=True, on_message_callback=callback)
            channel.start_consuming()
            connection.close()
        except Exception as e:
            event.fail(e)


if __name__ == "__main__":
    # Note: use_juju_for_storage=True required per
    # https://github.com/canonical/operator/issues/506
    main(PikaOperatorCharm, use_juju_for_storage=True)
