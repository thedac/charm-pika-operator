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
from ops.model import ActiveStatus, BlockedStatus, WaitingStatus

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
        self.framework.observe(self.on.update_status, self._on_update_status)

    def _on_pika_pebble_ready(self, event):
        self._on_update_status(event)

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
    def amqp_rel(self):
        return self.framework.model.get_relation("amqp")

    @property
    def password(self):
        return self.amqp_rel.data[self.amqp_rel.app].get("password")

    @property
    def hostname(self):
        return self.amqp_rel.data[self.amqp_rel.app].get("hostname")

    def _on_has_amqp_servers(self, event):
        logging.info("Requesting user and vhost")
        self._on_update_status(event)

    def _on_ready_amqp_servers(self, event):
        logging.info("Rabbitmq relation complete")
        self._on_update_status(event)
        self._on_query_amqp(event)

    def _on_config_changed(self, event):
        self._on_update_status(event)

    def _on_update_status(self, event):
        if not self.amqp_rel:
            self.unit.status = WaitingStatus("No AMQP relation yet")
        elif not (self.hostname and self.password):
            self.unit.status = WaitingStatus("AMQP relation not yet complete.")
        else:
            self.unit.status = ActiveStatus("Ready to run query-amqp action.")

    def _on_query_amqp(self, event):
        import pika

        logging.info("Attempting to query AMQP")
        try:
            credentials = credentials = pika.PlainCredentials(self.username, self.password)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.hostname, 5672, self.vhost, credentials))
            channel = connection.channel()

            # Purge queue
            # http://pika.readthedocs.org/en/latest/modules/channel.html?highlight=purge#pika.channel.Channel.queue_purge
            channel.queue_declare(queue=self.queue)

            channel.basic_publish(exchange='', routing_key=self.queue, body='Hello World!')
            logging.info("Sent 'Hello World!'")

            def callback(ch, method, properties, body):
                msg = "Success: Found '{}'".format(body.decode("UTF-8"))
                logging.info(msg)
                try:
                    # We are in an action
                    logging.info("Setting action results")
                    event.set_results({"result": msg})
                except AttributeError:
                    # Not an action
                    pass
                self.unit.status = ActiveStatus(msg)
                logging.warning("Exiting on purpose.")
                exit()

            channel.basic_consume(queue=self.queue, auto_ack=True, on_message_callback=callback)
            channel.start_consuming()
            connection.close()
        except Exception as e:
            try:
                # We are in an action
                event.fail(e)
            except AttributeError:
                # Not an action
                pass
            self.unit.status = BlockedStatus("Failed to send and recieve AMQP data. See logs.")


if __name__ == "__main__":
    # Note: use_juju_for_storage=True required per
    # https://github.com/canonical/operator/issues/506
    main(PikaOperatorCharm, use_juju_for_storage=True)
