import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path
from jinja2 import Environment, Undefined

class PromptRegistry:
    """
    Manages loading and rendering of prompt templates from YAML files.
    """

    def __init__(self, prompts_dir: str = "prompts"):
        self.prompts_dir = Path(prompts_dir)
        self._cache: Dict[str, Any] = {}
        # Jinja is used heavily in our prompt YAMLs (loops, conditionals).
        # Use a non-strict undefined to keep prompt rendering resilient.
        self._jinja = Environment(trim_blocks=True, lstrip_blocks=True, undefined=Undefined)

    def _render(self, template: str, variables: Dict[str, Any]) -> str:
        if template is None:
            return ""
        return self._jinja.from_string(str(template)).render(**(variables or {}))

    def load_prompt(self, prompt_name: str) -> Dict[str, Any]:
        """
        Loads a prompt definition from a YAML file.
        prompt_name format: 'category/name' (e.g., 'planner/intent_classification')
        """
        if prompt_name in self._cache:
            return self._cache[prompt_name]

        file_path = self.prompts_dir / f"{prompt_name}.yaml"
        
        if not file_path.exists():
            raise FileNotFoundError(f"Prompt file not found: {file_path}")

        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
            
        self._cache[prompt_name] = data
        return data

    def render_messages(self, prompt_name: str, variables: Dict[str, Any]) -> list[Dict[str, str]]:
        """
        Loads a prompt and substitutes variables into the messages.
        """
        prompt_def = self.load_prompt(prompt_name)

        # Preferred format in this repo: top-level 'system' and 'user' strings (often with Jinja).
        if "system" in prompt_def or "user" in prompt_def:
            rendered: list[Dict[str, str]] = []
            if "system" in prompt_def and str(prompt_def.get("system", "")).strip():
                rendered.append({"role": "system", "content": self._render(prompt_def.get("system"), variables)})
            if "user" in prompt_def and str(prompt_def.get("user", "")).strip():
                rendered.append({"role": "user", "content": self._render(prompt_def.get("user"), variables)})
            return rendered

        # Legacy formats: 'messages' array or 'system_prompt'/'user_prompt'
        messages = prompt_def.get("messages", [])
        if messages:
            return [{"role": msg["role"], "content": self._render(msg.get("content", ""), variables)} for msg in messages]

        rendered_messages: list[Dict[str, str]] = []
        if "system_prompt" in prompt_def and str(prompt_def.get("system_prompt", "")).strip():
            rendered_messages.append({"role": "system", "content": self._render(prompt_def.get("system_prompt"), variables)})
        if "user_prompt" in prompt_def and str(prompt_def.get("user_prompt", "")).strip():
            rendered_messages.append({"role": "user", "content": self._render(prompt_def.get("user_prompt"), variables)})
        return rendered_messages

    def get_config(self, prompt_name: str) -> Dict[str, Any]:
        """Returns the model config (model name, temperature, etc.)"""
        prompt_def = self.load_prompt(prompt_name)
        return {
            "model": prompt_def.get("model", "gpt-3.5-turbo"),
            "parameters": prompt_def.get("parameters", {})
        }

