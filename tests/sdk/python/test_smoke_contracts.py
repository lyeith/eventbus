"""SDK smoke must reject false success and preserve fixture ownership."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + ".py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def missing_pool() -> ClientError:
    return ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "AdminDeleteUser")


class SmokeContractTests(unittest.TestCase):
    def test_pool_delete_accepts_only_the_exact_service_failure(self) -> None:
        script = load("smoke_tier_c")
        client = Mock()
        client.admin_delete_user.side_effect = missing_pool()
        script.assert_pool_missing(client, "owned-pool", "fixture@example.test")
        client.admin_delete_user.assert_called_once_with(UserPoolId="owned-pool", Username="fixture@example.test")

    def test_success_network_failure_and_wrong_service_error_cannot_prove_deletion(self) -> None:
        for failure in [None, TimeoutError("network failure"), ClientError({"Error": {"Code": "AccessDeniedException"}}, "AdminDeleteUser")]:
            with self.subTest(failure=failure):
                script = load("smoke_tier_c")
                client = Mock()
                client.admin_delete_user.side_effect = failure
                with self.assertRaises((AssertionError, TimeoutError)):
                    script.assert_pool_missing(client, "owned-pool", "fixture@example.test")

    def test_client_creation_failure_cleans_up_only_its_owned_pool(self) -> None:
        script = load("smoke_tier_c")
        client = Mock()
        client.create_user_pool.return_value = {"UserPool": {"Id": "fresh-owner-id"}}
        client.create_user_pool_client.side_effect = ValueError("client creation failed")
        client.admin_delete_user.side_effect = missing_pool()
        with patch.object(script, "make_client", return_value=client), self.assertRaisesRegex(ValueError, "client creation failed"):
            script.main()
        self.assertEqual(set(client.create_user_pool.call_args.kwargs), {"PoolName"})
        client.delete_user_pool.assert_called_once_with(UserPoolId="fresh-owner-id")
        client.admin_create_user.assert_not_called()

    def test_initiate_collision_cannot_authenticate_or_delete_an_existing_user(self) -> None:
        script = load("smoke_initiate_auth")
        client = Mock()
        client.admin_create_user.side_effect = ClientError({"Error": {"Code": "UsernameExistsException"}}, "AdminCreateUser")
        with patch.object(script, "make_client", return_value=client), self.assertRaises(ClientError):
            script.main()
        client.admin_delete_user.assert_not_called()
        client.initiate_auth.assert_not_called()

    def test_sdk_network_waits_and_retry_count_are_bounded(self) -> None:
        for name in ["smoke_admin_ops", "smoke_initiate_auth", "smoke_tier_c"]:
            with self.subTest(script=name):
                script = load(name)
                create = Mock()
                with patch.object(script.boto3, "client", create):
                    script.make_client()
                config = create.call_args.kwargs["config"]
                self.assertEqual(config.connect_timeout, 2)
                self.assertEqual(config.read_timeout, 5)
                self.assertEqual(config.retries, {"total_max_attempts": 1})


if __name__ == "__main__":
    unittest.main()
