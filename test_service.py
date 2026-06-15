import unittest
from unittest.mock import patch, MagicMock
import requests as requests_lib


class TestRetryLogic(unittest.TestCase):
    def setUp(self):
        from durable import _infer_with_retry, RetryPolicy
        self.infer = _infer_with_retry
        self.RetryPolicy = RetryPolicy

    @patch("durable.requests.post")
    def test_success_on_first_attempt(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"prompt": "test", "response": "ok"}
        mock_post.return_value = mock_resp

        result = self.infer("http://mock:8081/infer", "test")
        self.assertEqual(result["response"], "ok")
        self.assertEqual(mock_post.call_count, 1)

    @patch("durable.requests.post")
    @patch("durable.time.sleep", return_value=None)
    def test_success_after_429_retry(self, mock_sleep, mock_post):
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"prompt": "test", "response": "ok"}
        mock_post.side_effect = [fail_resp, fail_resp, success_resp]

        result = self.infer("http://mock:8081/infer", "test")
        self.assertEqual(result["response"], "ok")
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("durable.requests.post")
    @patch("durable.time.sleep", return_value=None)
    def test_exponential_backoff_waits(self, mock_sleep, mock_post):
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"prompt": "test", "response": "ok"}
        mock_post.side_effect = [fail_resp, fail_resp, fail_resp, success_resp]

        self.infer("http://mock:8081/infer", "test")
        wait_times = [call[0][0] for call in mock_sleep.call_args_list]
        self.assertEqual(wait_times, [1, 2, 4])

    @patch("durable.requests.post")
    @patch("durable.time.sleep", return_value=None)
    def test_fails_after_max_retries(self, mock_sleep, mock_post):
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        mock_post.return_value = fail_resp

        result = self.infer("http://mock:8081/infer", "test")
        self.assertIn("FAILED after max retries", result["response"])
        self.assertEqual(mock_post.call_count, 5)
        self.assertEqual(mock_sleep.call_count, 5)

    @patch("durable.requests.post")
    @patch("durable.time.sleep", return_value=None)
    def test_retry_on_request_exception(self, mock_sleep, mock_post):
        mock_post.side_effect = [
            requests_lib.RequestException("Connection error"),
            requests_lib.RequestException("Timeout"),
            MagicMock(status_code=200, json=lambda: {"prompt": "test", "response": "ok"}),
        ]

        result = self.infer("http://mock:8081/infer", "test")
        self.assertEqual(result["response"], "ok")
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


class TestServerEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient
        from server import app
        cls.client = TestClient(app)

    def test_health_check(self):
        r = self.client.get("/")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), {"status": "ok"})

    def test_ingest_missing_field(self):
        r = self.client.post("/", json={})
        self.assertEqual(r.status_code, 422)

    def test_status_not_found(self):
        r = self.client.get("/status/nonexistent")
        self.assertEqual(r.json(), {"error": "not found"})

    def test_results_not_found(self):
        r = self.client.get("/results/nonexistent")
        self.assertEqual(r.json(), {"error": "not found or still processing"})

    def test_file_not_found(self):
        r = self.client.post("/from-file", json={"path": "/nonexistent/path.json"})
        self.assertEqual(r.status_code, 400)
        self.assertIn("File not found", r.json()["detail"])


class TestBatchIngestion(unittest.TestCase):
    def test_ingest_empty_batch(self):
        from server import ingest_batch, BatchInput
        import server
        with patch.object(server.threading.Thread, "start"):
            result = ingest_batch(BatchInput(prompts=[]))
            self.assertEqual(result["status"], "accepted")
            self.assertEqual(result["count"], 0)
            self.assertIn("batch_id", result)

    def test_ingest_with_prompts(self):
        from server import ingest_batch, BatchInput
        import server
        with patch.object(server.threading.Thread, "start"):
            result = ingest_batch(BatchInput(prompts=["hello"]))
            self.assertEqual(result["count"], 1)

    def test_file_ingestion_sample(self):
        from server import ingest_from_file, FileInput
        import server
        with patch.object(server.threading.Thread, "start"):
            result = ingest_from_file(FileInput(path="/workspaces/sample_prompts.json"))
            self.assertEqual(result["status"], "accepted")
            self.assertEqual(result["count"], 5)


class TestDurableStore(unittest.TestCase):
    def setUp(self):
        from durable import DurableStore
        self.store = DurableStore(":memory:")

    def test_create_and_get_status(self):
        self.store.create_orchestration("batch1", ["a", "b", "c"])
        status = self.store.get_status("batch1")
        self.assertEqual(status["batch_id"], "batch1")
        self.assertEqual(status["total"], 3)
        self.assertEqual(status["completed"], 0)
        self.assertEqual(status["status"], "running")

    def test_save_event_and_get_events(self):
        self.store.create_orchestration("batch2", ["x", "y"])
        self.store.save_event("batch2", 0, "infer", "x", {"result": "X"})
        self.store.save_event("batch2", 1, "infer", "y", {"result": "Y"})
        events = self.store.get_events("batch2")
        self.assertEqual(len(events), 2)
        status = self.store.get_status("batch2")
        self.assertEqual(status["completed"], 2)

    def test_complete_orchestration(self):
        self.store.create_orchestration("batch3", ["p"])
        self.store.complete_orchestration("batch3", [{"prompt": "p", "response": "r"}])
        status = self.store.get_status("batch3")
        self.assertEqual(status["status"], "completed")
        result = self.store.get_result("batch3")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["response"], "r")

    def test_fail_orchestration(self):
        self.store.create_orchestration("batch4", ["p"])
        self.store.fail_orchestration("batch4", "Something broke")
        status = self.store.get_status("batch4")
        self.assertEqual(status["status"], "failed")

    def test_get_result_nonexistent(self):
        self.assertIsNone(self.store.get_result("no-batch"))


class TestConcurrentOrchestration(unittest.TestCase):
    @patch("durable.requests.post")
    def test_run_orchestrator_success(self, mock_post):
        from durable import run_orchestrator

        def side_effect(*args, **kwargs):
            prompt = kwargs.get("json", {}).get("prompt", "")
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"prompt": prompt, "response": "r", "tokens_used": 10}
            return resp
        mock_post.side_effect = side_effect

        output = run_orchestrator("test-batch", ["a", "b", "c"], "http://mock:8081/infer")
        self.assertEqual(len(output), 3)
        self.assertEqual(output[0]["prompt"], "a")
        self.assertEqual(output[2]["prompt"], "c")

    @patch("durable.requests.post")
    @patch("durable.time.sleep", return_value=None)
    def test_run_orchestrator_with_retries(self, mock_sleep, mock_post):
        from durable import run_orchestrator
        mock_post.side_effect = [
            MagicMock(status_code=429),
            MagicMock(status_code=200, json=lambda: {"prompt": "x", "response": "ok"}),
        ]

        output = run_orchestrator("test-batch2", ["x"], "http://mock:8081/infer")
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0]["response"], "ok")
        self.assertEqual(mock_sleep.call_count, 1)


if __name__ == "__main__":
    unittest.main()
