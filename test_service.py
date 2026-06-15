import unittest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
import requests as requests_lib
from server import app, infer_with_retry, INFERENCE_URL

client = TestClient(app)


class TestHealthCheck(unittest.TestCase):
    def test_health_check(self):
        resp = client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"status": "ok"})


class TestIngestBatch(unittest.TestCase):
    @patch("server.requests.post")
    def test_ingest_empty_batch(self, mock_post):
        resp = client.post("/", json={"prompts": []})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "accepted")
        self.assertEqual(data["count"], 0)
        self.assertIn("batch_id", data)

    @patch("server.requests.post")
    def test_ingest_single_prompt(self, mock_post):
        def side_effect(*args, **kwargs):
            prompt = kwargs.get("json", {}).get("prompt", "")
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"prompt": prompt, "response": "ok", "tokens_used": 10}
            return resp
        mock_post.side_effect = side_effect

        resp = client.post("/", json={"prompts": ["Hello"]})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["count"], 1)

    @patch("server.requests.post")
    def test_ingest_multiple_prompts(self, mock_post):
        def side_effect(*args, **kwargs):
            prompt = kwargs.get("json", {}).get("prompt", "")
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"prompt": prompt, "response": "ok", "tokens_used": 10}
            return resp
        mock_post.side_effect = side_effect

        prompts = ["prompt-" + str(i) for i in range(10)]
        resp = client.post("/", json={"prompts": prompts})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["count"], 10)

    def test_ingest_missing_field(self):
        resp = client.post("/", json={})
        self.assertEqual(resp.status_code, 422)


class TestStatusAndResults(unittest.TestCase):
    def test_status_not_found(self):
        resp = client.get("/status/nonexistent")
        self.assertEqual(resp.json(), {"error": "not found"})

    def test_results_not_found(self):
        resp = client.get("/results/nonexistent")
        self.assertEqual(resp.json(), {"error": "not found"})


class TestFileIngestion(unittest.TestCase):
    def test_file_not_found(self):
        resp = client.post("/from-file", json={"path": "/nonexistent/path.json"})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("File not found", resp.json()["detail"])

    @patch("server.requests.post")
    def test_file_ingestion_sample(self, mock_post):
        def side_effect(*args, **kwargs):
            prompt = kwargs.get("json", {}).get("prompt", "")
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"prompt": prompt, "response": "ok", "tokens_used": 10}
            return resp
        mock_post.side_effect = side_effect

        resp = client.post("/from-file", json={"path": "/workspaces/sample_prompts.json"})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "accepted")
        self.assertEqual(data["count"], 5)


class TestRetryLogic(unittest.TestCase):
    @patch("server.requests.post")
    def test_success_on_first_attempt(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"prompt": "test", "response": "ok"}
        mock_post.return_value = mock_resp

        result = infer_with_retry("test")
        self.assertEqual(result["response"], "ok")
        self.assertEqual(mock_post.call_count, 1)

    @patch("server.requests.post")
    @patch("server.time.sleep", return_value=None)
    def test_success_after_429_retry(self, mock_sleep, mock_post):
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"prompt": "test", "response": "ok"}
        mock_post.side_effect = [fail_resp, fail_resp, success_resp]

        result = infer_with_retry("test")
        self.assertEqual(result["response"], "ok")
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("server.requests.post")
    @patch("server.time.sleep", return_value=None)
    def test_exponential_backoff_waits(self, mock_sleep, mock_post):
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"prompt": "test", "response": "ok"}
        mock_post.side_effect = [fail_resp, fail_resp, fail_resp, success_resp]

        infer_with_retry("test")
        wait_times = [call[0][0] for call in mock_sleep.call_args_list]
        self.assertEqual(wait_times, [1, 2, 4])

    @patch("server.requests.post")
    @patch("server.time.sleep", return_value=None)
    def test_fails_after_max_retries(self, mock_sleep, mock_post):
        fail_resp = MagicMock()
        fail_resp.status_code = 429
        mock_post.return_value = fail_resp

        result = infer_with_retry("test")
        self.assertIn("FAILED after max retries", result["response"])
        self.assertEqual(mock_post.call_count, 5)
        self.assertEqual(mock_sleep.call_count, 5)

    @patch("server.requests.post")
    @patch("server.time.sleep", return_value=None)
    def test_retry_on_request_exception(self, mock_sleep, mock_post):
        mock_post.side_effect = [
            requests_lib.RequestException("Connection error"),
            requests_lib.RequestException("Timeout"),
            MagicMock(status_code=200, json=lambda: {"prompt": "test", "response": "ok"}),
        ]

        result = infer_with_retry("test")
        self.assertEqual(result["response"], "ok")
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


class TestIntegration(unittest.TestCase):
    @patch("server.requests.post")
    def test_full_batch_lifecycle(self, mock_post):
        def side_effect(*args, **kwargs):
            prompt = kwargs.get("json", {}).get("prompt", "")
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"prompt": prompt, "response": "r", "tokens_used": 10}
            return resp
        mock_post.side_effect = side_effect

        import server
        import threading

        batch_id = "testbatch"
        prompts = ["a", "b", "c"]
        thread = threading.Thread(target=server.process_batch, args=(batch_id, prompts))
        thread.start()
        thread.join()

        results = server.results.get(batch_id)
        self.assertIsNotNone(results)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["prompt"], "a")

        with server.status_lock:
            status = server.status_map[batch_id]
        self.assertEqual(status["completed"], 3)
        self.assertEqual(status["total"], 3)
        self.assertEqual(status["status"], "completed")


if __name__ == "__main__":
    unittest.main()
