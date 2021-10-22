#!/usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

"""Module defining a Charm providing database management for FINOS Legend."""

import logging

from charms.finos_legend_db_k8s.v0 import legend_database
from charms.mongodb_k8s.v0.mongodb import MongoConsumer
from ops import charm, framework, main, model

logger = logging.getLogger(__name__)

MONGODB_RELATION_NAME = "db"
LEGEND_DB_RELATION_NAME = "legend-db"


class LegendDatabaseManagerCharm(charm.CharmBase):
    """Charm which shares a MongodDB relation with related Legend Services."""

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
            self.on[MONGODB_RELATION_NAME].relation_joined, self._on_db_relation_joined
        )
        self.framework.observe(
            self.on[MONGODB_RELATION_NAME].relation_changed, self._on_db_relation_changed
        )

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

    def _on_install(self, _: charm.InstallEvent):
        self.unit.status = model.BlockedStatus("requires relating to: mongodb-k8s")

    def _on_config_changed(self, _) -> None:
        # NOTE(aznashwan): this charm does not yet have any config options:
        pass

    def _on_db_relation_joined(self, event: charm.RelationJoinedEvent):
        pass

    def _set_legend_db_creds_in_relation(self, legend_database_creds, relation):
        """Attempts to add the given Database creds to the given relation's data.

        Returns a `model.BlockedStatus` if it was unable to set the rel data.
        """
        if not legend_database.set_legend_database_creds_in_relation_data(
            relation.data[self.app], legend_database_creds
        ):
            return model.BlockedStatus(
                "failed to set creds in legend db relation: %s" % (relation.id)
            )
        return None

    def _set_legend_db_creds_in_relations(self, legend_database_creds):
        """Shares the MongoDB creds with all related Lenged services.

        Returns a `model.BlockedStatus` if it was unable to set the rel data.
        """
        for relation in self.model.relations[LEGEND_DB_RELATION_NAME]:
            possible_blocked_status = self._set_legend_db_creds_in_relation(
                legend_database_creds, relation
            )
            if possible_blocked_status:
                return possible_blocked_status
        return None

    def _get_mongo_db_credentials(self, rel_id=None):
        """Returns MongoDB creds or a `Waiting/BlockedStatus` otherwise."""
        # Check whether credentials for a database are available:
        mongo_creds = self._mongodb_consumer.credentials(rel_id)
        if not mongo_creds:
            return model.WaitingStatus("waiting for mongo database credentials")

        # Check whether the databases were created:
        databases = self._mongodb_consumer.databases(rel_id)
        if not databases:
            self._mongodb_consumer.new_database()
            return model.WaitingStatus("waiting for mongo database creation")

        # Fetch the credentials from the relation data:
        get_creds = legend_database.get_database_connection_from_mongo_data
        legend_database_creds = get_creds(mongo_creds, databases)
        if not legend_database_creds:
            return model.BlockedStatus(
                "failed to process MongoDB connection data for legend db "
                "format, please review the debug-log for full details"
            )
        logger.debug(
            "Current Legend MongoDB creds provided by the relation are: %s", legend_database_creds
        )

        return legend_database_creds

    def _on_db_relation_changed(self, event: charm.RelationChangedEvent) -> None:
        rel_id = event.relation.id
        legend_database_creds = self._get_mongo_db_credentials(rel_id)
        if isinstance(legend_database_creds, (model.WaitingStatus, model.BlockedStatus)):
            self.unit.status = legend_database_creds
            return

        # Propagate the DB creds to all relating Legend services:
        possible_blocked_status = self._set_legend_db_creds_in_relations(legend_database_creds)
        if possible_blocked_status:
            self.unit.status = possible_blocked_status
            return

        self.unit.status = model.ActiveStatus()

    def _on_legend_db_relation_joined(self, event: charm.RelationJoinedEvent):
        legend_database_creds = self._get_mongo_db_credentials()
        if isinstance(legend_database_creds, (model.WaitingStatus, model.BlockedStatus)):
            logger.warning(
                "Could not provide Legend MongoDB creds to relation '%s' as none are "
                "currently available",
                event.relation.id,
            )
            self.unit.status = legend_database_creds
            return

        # Add the creds to the relation:
        possible_blocked_status = self._set_legend_db_creds_in_relation(
            legend_database_creds, event.relation
        )
        if possible_blocked_status:
            self.unit.status = possible_blocked_status
            return
        logger.debug("test")

    def _on_legend_db_relation_changed(self, event: charm.RelationChangedEvent):
        pass


if __name__ == "__main__":
    main.main(LegendDatabaseManagerCharm)
