#!/usr/bin/env python3
# Copyright 2021 Canonical
# See LICENSE file for licensing details.

""" Module defining a Charm providing database management for FINOS Legend. """

import functools
import json
import logging

from ops import charm
from ops import framework
from ops import main
from ops import model

from charms.mongodb_k8s.v0 import mongodb

LOG = logging.getLogger(__name__)


def _logged_charm_entry_point(fun):
    """ Add logging for method call/exits. """
    @functools.wraps(fun)
    def _inner(*args, **kwargs):
        LOG.info(
            "### Initiating Legend DBAdmin charm call to '%s'", fun.__name__)
        res = fun(*args, **kwargs)
        LOG.info(
            "### Completed Legend DBAdmin charm call to '%s'", fun.__name__)
        return res
    return _inner


class LegendDatabaseManagerCharm(charm.CharmBase):
    """Charm class which exposes the MongoDB it is related to to the
    FINOS Legend Charmed Operators. """

    _stored = framework.StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self._set_stored_defaults()

        # MongoDB consumer setup:
        self._mongodb_consumer = mongodb.MongoConsumer(
            self, "db", {"mongodb": ">=4.0"}, multi=False)

        # Mongo relation lifecycle events:
        self.framework.observe(
            self.on["db"].relation_joined,
            self._on_db_relation_joined)
        self.framework.observe(
            self.on["db"].relation_changed,
            self._on_db_relation_changed)

        # Legend component relation events:
        self.framework.observe(
            self.on["legend-db"].relation_joined,
            self._on_legend_db_relation_joined)
        self.framework.observe(
            self.on["legend-db"].relation_changed,
            self._on_legend_db_relation_changed)

        # Set blocked status until MongoDB is realted:
        if not self.unit.status:
            self.unit.status = model.BlockedStatus(
                "Requires relating to MongoDB.")

    def _set_stored_defaults(self) -> None:
        self._stored.set_default(log_level="DEBUG")
        self._stored.set_default(mongodb_credentials={})

    @_logged_charm_entry_point
    def _on_config_changed(self, _) -> None:
        """Reacts to configuration changes to the service by:
        - regenerating the JSON config for the Engine service
        - adding it via Pebble
        - instructing Pebble to restart the Engine service
        """

    @_logged_charm_entry_point
    def _on_db_relation_joined(self, event: charm.RelationJoinedEvent):
        self.unit.status = model.WaitingStatus(
            "Waiting for MongoDB database creation.")

    @_logged_charm_entry_point
    def _on_db_relation_changed(
            self, event: charm.RelationChangedEvent) -> None:
        # _ = self.model.get_relation(event.relation.name, event.relation.id)
        rel_id = event.relation.id

        # Check whether credentials for a database are available:
        mongo_creds = self._mongodb_consumer.credentials(rel_id)
        if not mongo_creds:
            LOG.info(
                "No MongoDB database credentials present in relation. "
                "Returning now to await their availability.")
            self.unit.status = model.WaitingStatus(
                "Waiting for MongoDB database credentials.")
            return
        LOG.info(
            "Current MongoDB database creds provided by relation are: %s",
            mongo_creds)

        # Check whether the databases were created:
        databases = self._mongodb_consumer.databases(rel_id)
        if not databases:
            LOG.info(
                "No MongoDB database currently present in relation. "
                "Requesting creation now.")
            self._mongodb_consumer.new_database()
            self.unit.status = model.WaitingStatus(
                "Waiting for MongoDB database creation.")
            return
        LOG.info(
            "Current MongoDB databases provided by the relation are: %s",
            databases)
        # NOTE(aznashwan): we hackily add the databases in here too:
        mongo_creds['databases'] = databases
        self._stored.mongodb_credentials = mongo_creds

        self.unit.status = model.ActiveStatus(
            "Ready to be related to Legend components.")

    @_logged_charm_entry_point
    def _on_legend_db_relation_joined(self, event: charm.RelationJoinedEvent):
        mongo_creds = self._stored.mongodb_credentials
        if not mongo_creds:
            self.unit.status = model.BlockedStatus(
                "MongoDB relation is required to pass on creds to Legend "
                "components.")
            return

        rel_id = event.relation.id
        rel = self.framework.model.get_relation("legend-db", rel_id)
        rel.data[self.app] = json.dumps({"legend-db-connection": mongo_creds})

    @_logged_charm_entry_point
    def _on_legend_db_relation_changed(
            self, event: charm.RelationChangedEvent):
        pass


if __name__ == "__main__":
    main.main(LegendDatabaseManagerCharm)
