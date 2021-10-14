# Copyright 2021 Canonical
# See LICENSE file for licensing details.

import json
import unittest

from charms.finos_legend_db_k8s.v0 import legend_database
from ops import charm as ops_charm
from ops import testing as ops_testing

import charm


class LegendDBConsumerTestCharm(ops_charm.CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.legend_db_consumer = legend_database.LegendDatabaseConsumer(
            self, charm.LEGEND_DB_RELATION_NAME
        )


class TestLegendDBConsumer(unittest.TestCase):
    def setUp(self):
        self.harness = ops_testing.Harness(
            LegendDBConsumerTestCharm,
            meta="""
            name: legend-db-test
            requires:
              legend-db:
                interface: legend_mongodb
        """,
        )
        self.addCleanup(self.harness.cleanup)

    def _add_db_relation(self, relator_name, relation_data):
        rel_id = self.harness.add_relation(charm.LEGEND_DB_RELATION_NAME, relator_name)
        self.harness.add_relation_unit(rel_id, relator_name)
        self.harness.update_relation_data(rel_id, relator_name, relation_data)
        return rel_id

    def test_validate_legend_database_creds(self):
        # Wrong type:
        self.assertFalse(legend_database._validate_legend_database_credentials(None))
        self.assertFalse(legend_database._validate_legend_database_credentials(13))
        self.assertFalse(legend_database._validate_legend_database_credentials(["list"]))

        # Empty dict
        self.assertFalse(legend_database._validate_legend_database_credentials({}))

        # Dict with all keys present:
        self.assertTrue(
            legend_database._validate_legend_database_credentials(
                {k: k for k in legend_database.REQUIRED_LEGEND_DATABASE_CREDENTIALS}
            )
        )

    def test_get_database_connection_from_mongo_data(self):
        db = "testdb"
        pwd = "testpass"
        user = "testuser"
        uri = "mongodb://u:p@host:27017/%s" % db

        # Proper test:
        res = legend_database.get_database_connection_from_mongo_data(
            {"username": user, "password": pwd, "replica_set_uri": uri}, [db]
        )
        self.assertIsInstance(res, dict)
        self.assertEqual(
            res,
            {
                # NOTE: consumer returns the Mongo URI minus the DB name:
                "uri": uri[: len(uri) - len(db) - 1],
                "username": user,
                "password": pwd,
                "database": db,
            },
        )

        # Nones:
        self.assertFalse(legend_database.get_database_connection_from_mongo_data(None, None))
        # Empty creds:
        self.assertFalse(legend_database.get_database_connection_from_mongo_data({}, None))
        # Missing Keys:
        self.assertFalse(
            legend_database.get_database_connection_from_mongo_data(
                {"username": "u", "password": "p"}, None
            )
        )
        # Bad databases param type:
        self.assertFalse(
            legend_database.get_database_connection_from_mongo_data(
                {"username": user, "password": pwd, "replica_set_uri": uri}, 13
            )
        )
        # Empty databases:
        self.assertFalse(
            legend_database.get_database_connection_from_mongo_data(
                {"username": user, "password": pwd, "replica_set_uri": uri}, []
            )
        )
        # Bad elem in databases:
        self.assertFalse(
            legend_database.get_database_connection_from_mongo_data(
                {"username": user, "password": pwd, "replica_set_uri": uri}, [13]
            )
        )
        # Bad type in creds:
        self.assertFalse(
            legend_database.get_database_connection_from_mongo_data(
                {"username": 13, "password": pwd, "replica_set_uri": uri}, [db]
            )
        )
        # Unsplittable replica set URI:
        self.assertFalse(
            legend_database.get_database_connection_from_mongo_data(
                {"username": user, "password": pwd, "replica_set_uri": "unsplittable"}, [db]
            )
        )

    def test_set_legend_database_creds_in_relation_data(self):
        # Invalid:
        rel_data = {}
        creds = {"totally": "invalid"}
        self.assertFalse(
            legend_database.set_legend_database_creds_in_relation_data(rel_data, creds)
        )
        self.assertEqual(rel_data, {})

        # Valid:
        rel_data = {}
        creds = {
            "uri": "testuri",
            "username": "testuser",
            "password": "testpass",
            "database": "testdb",
        }
        self.assertTrue(
            legend_database.set_legend_database_creds_in_relation_data(rel_data, creds)
        )
        set_creds = rel_data.get(legend_database.LEGEND_DB_RELATION_DATA_KEY)
        self.assertEqual(json.loads(set_creds), creds)

    def test_get_legend_creds_no_relation(self):
        self.harness.begin_with_initial_hooks()
        res = self.harness.charm.legend_db_consumer.get_legend_database_creds(None)
        self.assertEqual(res, {})

    def test_get_legend_creds(self):
        rel_data = {}
        creds = {
            "uri": "testuri",
            "username": "testuser",
            "password": "testpass",
            "database": "testdb",
        }
        self.assertTrue(
            legend_database.set_legend_database_creds_in_relation_data(rel_data, creds)
        )

        rel_id = self._add_db_relation("test_relator", rel_data)
        self.harness.begin_with_initial_hooks()

        res = self.harness.charm.legend_db_consumer.get_legend_database_creds(rel_id)
        self.assertEqual(res, creds)
