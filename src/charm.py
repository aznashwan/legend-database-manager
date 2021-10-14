#!/usr/bin/env python3
# Copyright 2021 Canonical
# See LICENSE file for licensing details.

""" Module defining a Charm providing database management for FINOS Legend. """

import logging

from charms.finos_legend_db_k8s.v0 import legend_database
from charms.mongodb_k8s.v0.mongodb import MongoConsumer
from ops import charm, framework, main, model

logger = logging.getLogger(__name__)

MONGODB_RELATION_NAME = "db"
LEGEND_DB_RELATION_NAME = "legend-db"


class LegendDatabaseManagerCharm(charm.CharmBase):
    """Charm class which exposes the MongoDB it is related to to the
    FINOS Legend Charmed Operators."""

    _stored = framework.StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self._set_stored_defaults()

        # General hooks:
        self.framework.observe(self.on.install, self._on_install)

        # MongoDB consumer setup:
        self._mongodb_consumer = MongoConsumer(self, MONGODB_RELATION_NAME)

        # LDB library setup:
        self._legend_db_consumer = legend_database.LegendDatabaseConsumer(self)

        # Mongo relation lifecycle events:
        self.framework.observe(
            self.on[MONGODB_RELATION_NAME].relation_joined, self._on_db_relation_joined)
        self.framework.observe(
            self.on[MONGODB_RELATION_NAME].relation_changed, self._on_db_relation_changed)

        # Legend component relation events:
        self.framework.observe(
            self.on[LEGEND_DB_RELATION_NAME].relation_joined, self._on_legend_db_relation_joined
        )
        self.framework.observe(
            self.on[LEGEND_DB_RELATION_NAME].relation_changed, self._on_legend_db_relation_changed
        )

        # Set blocked status until MongoDB is realted:
        if not self.unit.status:
            self.unit.status = model.BlockedStatus("requires relating to: mongodb-k8s")

    def _set_stored_defaults(self) -> None:
        self._stored.set_default(log_level="DEBUG")

    def _on_install(self, event: charm.InstallEvent):
        self.unit.status = model.BlockedStatus("requires relating to: mongodb-k8s")

    def _on_config_changed(self, _) -> None:
        # NOTE(aznashwan): this charm does not yet have any config options:
        pass

    def _on_db_relation_joined(self, event: charm.RelationJoinedEvent):
        pass

    def _set_legend_db_creds_in_relations(self, legend_database_creds):
        """Attempts to add the given Database creds to the relation data
        of all related Legend services.
        Returns a `model.BlockedStatus` if it was unable to set the rel data.
        """
        for relation in self.model.relations[LEGEND_DB_RELATION_NAME]:
            if not legend_database.set_legend_database_creds_in_relation_data(
                relation.data[self.app], legend_database_creds
            ):
                return model.BlockedStatus(
                    "failed to set creds in legend db relation: %s" % (relation.id)
                )
        return None

    def _on_db_relation_changed(self, event: charm.RelationChangedEvent) -> None:
        rel_id = event.relation.id

        # Check whether credentials for a database are available:
        mongo_creds = self._mongodb_consumer.credentials(rel_id)
        if not mongo_creds:
            self.unit.status = model.WaitingStatus("waiting for mongo database credentials")
            return

        # Check whether the databases were created:
        databases = self._mongodb_consumer.databases(rel_id)
        if not databases:
            self._mongodb_consumer.new_database()
            self.unit.status = model.WaitingStatus("waiting for mongo database creation")
            return

        # Fetch the credentials from the relation data:
        get_creds = legend_database.get_database_connection_from_mongo_data
        legend_database_creds = get_creds(mongo_creds, databases)
        if not legend_database_creds:
            self.unit.status = model.BlockedStatus(
                "failed to process MongoDB connection data for legend db "
                "format, please review the debug-log for full details"
            )
            return
        logger.debug(
            "Current Legend MongoDB creds provided by the relation are: %s", legend_database_creds
        )

        # Propagate the DB creds to all relating Legend services:
        possible_blocked_status = self._set_legend_db_creds_in_relations(legend_database_creds)
        if possible_blocked_status:
            self.unit.status = possible_blocked_status
            return

        self.unit.status = model.ActiveStatus()

    def _on_legend_db_relation_joined(self, event: charm.RelationJoinedEvent):
        pass

    def _on_legend_db_relation_changed(self, event: charm.RelationChangedEvent):
        pass


if __name__ == "__main__":
    main.main(LegendDatabaseManagerCharm)
