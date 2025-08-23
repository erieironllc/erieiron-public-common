import json
from enum import StrEnum, auto
from pathlib import Path

import openai


class LlmIntelligence(StrEnum):
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()


def llm_chat(
        tag: str,
        llm_intelligence: LlmIntelligence,
        system_prompt: str,
        user_prompts: list[str],
        response_format: str = None
):
    """
    LLM chat helper.

    Purpose:
    - Wraps the OpenAI Responses API to send a system prompt and one or more user prompts in a single call.
    - Selects the model and reasoning_effort based on llm_intelligence.
    - Optionally enforces a JSON schema via response_format supplied as a Path, dict, or JSON string.
    - Applies a normalized billing tag to request metadata and the optional user field.

    Parameters:
    - tag: str
      Arbitrary tag used for billing and analytics. Normalized to lowercase ascii with underscores and hyphens. Max length 64.
    - llm_intelligence: LlmIntelligence
      LOW uses gpt-5-mini. MEDIUM uses gpt-5. HIGH uses gpt-5 with reasoning.effort set to "high".
    - system_prompt: str
      Content for the system role. Pass an empty string or None to omit.
    - user_prompts: list[str]
      One or more user role messages. Non strings are coerced with str. None entries become empty strings.
    - response_format: str | dict | pathlib.Path | None
      JSON schema to enforce on the model response. If Path, the file must exist and contain a JSON schema. If str, it must be a JSON string.

    Returns:
    - dict if response_format is supplied, str if response_format is not supplied
      The assistant text returned by Responses API as output_text.

    Behavior notes:
    - Secrets are resolved via erieiron_public.agent_tools.get_secret_from_env_arn and the OPENAI key is used to construct the client.
    - Messages are sent using Responses API with input set to the assembled role messages.
    - If tag is provided, a safe tag is written to kwargs.metadata.billing_tag and kwargs.user if user is not already supplied.

    Error handling:
    - If a Path is provided that does not exist, an error is logged and response_format is ignored.
    - If response_format is a string that fails JSON parsing, the exception is logged and response_format is ignored.

    Example:
    >>> llm_chat("billing-demo", LlmIntelligence.MEDIUM, "You are terse.", ["Say hi"], None)
    'Hi.'
    """
    from erieiron_public.agent_tools import get_secret_from_env_arn
    
    llm_api_keys_dict = get_secret_from_env_arn("LLM_API_KEYS_SECRET_ARN")
    client = openai.OpenAI(api_key=llm_api_keys_dict['OPENAI'])
    
    llm_intelligence = LlmIntelligence(llm_intelligence)
    if llm_intelligence == LlmIntelligence.HIGH:
        model_name = "gpt-5"
        reasoning_effort = "high"
    elif llm_intelligence == LlmIntelligence.LOW:
        model_name = "gpt-5-mini"
        reasoning_effort = None
    else:
        model_name = "gpt-5"
        reasoning_effort = None
    
    messages = []
    if system_prompt:
        messages.append(
            {
                "role": "system",
                "content": system_prompt
            })
    
    messages += [
        {
            "role": "user",
            "content": up
        }
        for up in _ensure_str_list(user_prompts)
    ]
    
    kwargs = {
        "model": model_name,
        "input": messages,
    }
    
    if response_format:
        if isinstance(response_format, Path):
            if response_format.exists():
                kwargs["response_format"] = {
                    "type": "json_schema",
                    "json_schema": json.loads(Path(response_format).read_text())
                }
            else:
                raise Exception(f"{response_format} does not exist")
        elif isinstance(response_format, dict):
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": response_format
            }
        else:
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": json.loads(response_format)
            }

    if reasoning_effort:
        # OpenAI Responses API expects reasoning set under the "reasoning" object
        # Example: {"reasoning": {"effort": "high"}}
        kwargs["reasoning"] = {"effort": reasoning_effort}
    
    # Apply request tag for billing/analytics
    if tag:
        safe_tag = _normalize_tag(tag)
        # Attach to metadata for downstream usage aggregation
        kwargs["metadata"] = {**kwargs.get("metadata", {}), "billing_tag": safe_tag}
        # Also set the OpenAI 'user' field to aid abuse monitoring and per-user usage views
        # Only set if not already provided by caller
        kwargs.setdefault("user", safe_tag)
    
    if response_format:
        return client.responses.create(**kwargs).output_json
    else:
        return client.responses.create(**kwargs).output_text


def _ensure_str_list(user_prompts) -> list[str]:
    if user_prompts is None:
        return []
    if isinstance(user_prompts, str):
        return [user_prompts]
    if isinstance(user_prompts, (list, tuple, set)):
        return ["" if p is None else str(p) for p in user_prompts]
    return [str(user_prompts)]


def _normalize_tag(value: str) -> str:
    """Return a safe, bounded tag: lowercase, ascii, hyphen/underscore allowed, max 64 chars."""
    if value is None:
        return ""
    s = str(value).strip().lower()
    # Replace whitespace with underscores
    s = "_".join(s.split())
    # Keep only allowed characters: a-z, 0-9, underscore, hyphen
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789_-")
    s = "".join(ch for ch in s if ch in allowed)
    # Enforce max length
    return s[:64] or "untagged"
