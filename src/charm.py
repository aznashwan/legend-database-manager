#!/usr/bin/env python3
# Copyright 2021 Canonical
# See LICENSE file for licensing details.

""" Module defining a Charm providing database management for FINOS Legend. """

import json
import logging

from ops import charm
from ops import framework
from ops import main
from ops import model

from charms.mongodb_k8s.v0 import mongodb
from charms.finos_legend_db_k8s.v0 import legend_database


logger = logging.getLogger(__name__)


class LegendDatabaseManagerCharm(charm.CharmBase):
    """Charm class which exposes the MongoDB it is related to to the
    FINOS Legend Charmed Operators. """

    _stored = framework.StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self._set_stored_defaults()

        # MongoDB consumer setup:
        self._mongodb_consumer = mongodb.MongoConsumer(self, "db")

        # LDB library setup:
        self._legend_db_consumer = legend_database.LegendDatabaseConsumer(self)

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
        self._stored.set_default(legend_db_conn_json="{}")

    def _on_config_changed(self, _) -> None:
        # NOTE(aznashwan): this charm does not yet have any config options:
        pass

    def _on_db_relation_joined(self, event: charm.RelationJoinedEvent):
        self.unit.status = model.WaitingStatus(
            "Waiting for MongoDB database creation.")

    def _on_db_relation_changed(
            self, event: charm.RelationChangedEvent) -> None:
        rel_id = event.relation.id

        # Check whether credentials for a database are available:
        mongo_creds = self._mongodb_consumer.credentials(rel_id)
        if not mongo_creds:
            self.unit.status = model.WaitingStatus(
                "Waiting for MongoDB database credentials.")
            return

        # Check whether the databases were created:
        databases = self._mongodb_consumer.databases(rel_id)
        if not databases:
            self._mongodb_consumer.new_database()
            self.unit.status = model.WaitingStatus(
                "Waiting for MongoDB database creation.")
            return

        get_creds = legend_database.get_database_connection_from_mongo_data
        legend_database_creds = get_creds(mongo_creds, databases)
        if not legend_database_creds:
            self.unit.status = model.BlockedStatus(
                "Failed to process MongoDB connection data for Legend DB "
                "format. Please review the debug-log for full details.")
            return
        logger.debug(
            "Current Legend MongoDB creds provided by the relation are: %s",
            legend_database_creds)

        # NOTE: we store/load the creds as JSON to avoid dealing with
        # the various wrapper types like `StoredDict`
        self._stored.legend_db_conn_json = json.dumps(
            legend_database_creds)
        self.unit.status = model.ActiveStatus(
            "Ready to be related to Legend components.")

    def _on_legend_db_relation_joined(self, event: charm.RelationJoinedEvent):
        mongo_creds = json.loads(str(self._stored.legend_db_conn_json))
        if not mongo_creds:
            self.unit.status = model.BlockedStatus(
                "MongoDB relation is required to pass on creds to Legend "
                "components.")
            return

        if not legend_database.set_legend_database_creds_in_relation_data(
                event.relation.data[self.app], mongo_creds):
            self.unit.status = model.BlockedStatus(
                "Failed to set creds in Legend DB relation: %s" % mongo_creds)
            return

    def _on_legend_db_relation_changed(
            self, event: charm.RelationChangedEvent):
        pass


if __name__ == "__main__":
    main.main(LegendDatabaseManagerCharm)
