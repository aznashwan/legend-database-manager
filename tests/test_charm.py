# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import unittest
from unittest import mock

from ops import model
from ops import testing as ops_testing

import charm


class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = ops_testing.Harness(charm.LegendDatabaseManagerCharm)
        self.addCleanup(self.harness.cleanup)

    def _add_mongo_relation(self, relation_data):
        """Adds a MongoDB relation and puts the given data within it."""
        relator = "mongodb-k8s"
        rel_id = self.harness.add_relation(charm.MONGODB_RELATION_NAME, relator)
        relator_unit = "%s/0" % relator
        self.harness.add_relation_unit(rel_id, relator_unit)
        self.harness.update_relation_data(rel_id, relator_unit, relation_data)
        return rel_id

    def _add_consumer_relation(self, relator_name, relation_data):
        rel_id = self.harness.add_relation(charm.LEGEND_DB_RELATION_NAME, relator_name)
        relator_unit = "%s/0" % relator_name
        self.harness.add_relation_unit(rel_id, relator_unit)
        self.harness.update_relation_data(rel_id, relator_unit, relation_data)
        return rel_id

    def _mock_mongo_consumer_cls(self, credentials_returns, databases_returns):
        mongo_consumer = mock.MagicMock()
        mongo_consumer.databases.return_value = databases_returns
        mongo_consumer.credentials.return_value = credentials_returns
        return mongo_consumer

    @mock.patch("charm.MongoConsumer")
    def test_mongo_relation_waiting_creds(self, _mongo_consumer_cls):
        mongo_consumer_mock = self._mock_mongo_consumer_cls({}, [])
        _mongo_consumer_cls.return_value = mongo_consumer_mock

        rel_id = self._add_mongo_relation({})
        self.harness.set_leader()
        self.harness.begin_with_initial_hooks()

        self.assertIsInstance(self.harness.charm.unit.status, model.WaitingStatus)
        self.assertEqual(
            self.harness.charm.unit.status.message, "waiting for mongo database credentials"
        )
        mongo_consumer_mock.credentials.assert_called_once_with(rel_id)

    @mock.patch("charm.MongoConsumer")
    def test_mongo_relation_waiting_databases(self, _mongo_consumer_cls):
        mongo_consumer_mock = self._mock_mongo_consumer_cls({"anything": "works"}, [])
        _mongo_consumer_cls.return_value = mongo_consumer_mock

        rel_id = self._add_mongo_relation({})
        self.harness.set_leader()
        self.harness.begin_with_initial_hooks()

        self.assertIsInstance(self.harness.charm.unit.status, model.WaitingStatus)
        self.assertEqual(
            self.harness.charm.unit.status.message, "waiting for mongo database creation"
        )
        mongo_consumer_mock.credentials.assert_called_once_with(rel_id)
        mongo_consumer_mock.databases.assert_called_once_with(rel_id)

    @mock.patch("charm.MongoConsumer")
    @mock.patch(
        "charms.finos_legend_db_k8s.v0.legend_database.get_database_connection_from_mongo_data"
    )
    @mock.patch(
        "charms.finos_legend_db_k8s.v0.legend_database.set_legend_database_creds_in_relation_data"
    )
    def test_mongo_relation_established(
        self, _set_rel_cred_mock, _get_rel_creds_mock, _mongo_consumer_cls_mock
    ):
        testing_database = "testdb"
        mongodb_test_creds = {"testing": "credentials"}
        mongo_consumer_mock = self._mock_mongo_consumer_cls(mongodb_test_creds, [testing_database])
        _mongo_consumer_cls_mock.return_value = mongo_consumer_mock

        _get_rel_creds_mock.return_value = mongodb_test_creds

        _ = self._add_consumer_relation("some-relator", {})
        mongo_rel_id = self._add_mongo_relation({"anything": "works"})
        self.harness.set_leader()
        self.harness.begin_with_initial_hooks()

        self.assertIsInstance(self.harness.charm.unit.status, model.ActiveStatus)
        mongo_consumer_mock.credentials.assert_has_calls(
            # NOTE(aznashwan): the call from the mongo relation-changed and legend
            # relation-joined could come in any order:
            [mock.call(mongo_rel_id), mock.call(None)],
            any_order=True,
        )
        mongo_consumer_mock.databases.assert_has_calls(
            [mock.call(None), mock.call(mongo_rel_id)], any_order=True
        )
        _get_rel_creds_mock.assert_has_calls(
            [mock.call(mongodb_test_creds, [testing_database])] * 2
        )
        _set_rel_cred_mock.assert_has_calls(
            [mock.call({}, mongodb_test_creds)] * 2, any_order=True
        )
