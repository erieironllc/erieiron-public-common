import os
from unittest import TestCase
from unittest.mock import patch

from erieiron_public import erieiron_public_common
from erieiron_public.agent_chat import llm_chat, LlmIntelligence


class TestChat(TestCase):
    @patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-west-2"})
    def test_chat(self):
        secret_arn = erieiron_public_common.get_secret_arn("LLM_API_KEYS")
        
        with patch.dict(os.environ, {"LLM_API_KEYS_SECRET_ARN": secret_arn}):
            # Environment variable is only set within this context
            self.assertEquals("hi", llm_chat("test_chat", LlmIntelligence.HIGH, "you respond briefly with no punctuation", "say Hi").lower())
            self.assertEquals("hi", llm_chat("test_chat", LlmIntelligence.MEDIUM, "you respond briefly with no punctuation", "say Hi").lower())
            self.assertEquals("hi", llm_chat("test_chat", LlmIntelligence.LOW, "you respond briefly with no punctuation", "say Hi").lower())

        
