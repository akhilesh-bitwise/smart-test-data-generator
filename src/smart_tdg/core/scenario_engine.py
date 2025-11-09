"""Scenario Engine for NL to YAML scenario generation."""

from typing import Optional, Dict
from openai import OpenAI
import yaml
from utils.config import Config

class ScenarioEngine:
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o-mini", temperature: float = 0.7):
        self.api_key = api_key or Config.OPENAI_API_KEY
        if self.api_key is None:
            raise ValueError("OpenAI API key not provided or set in environment")
        # openai.api_key = self.api_key
        self.client = OpenAI(api_key=self.api_key)
        self.model = model
        self.temperature = temperature
    
    def generate_scenario(self, nl_prompt: str) -> Dict:
        """Generate YAML scenario dict from natural language prompt."""
        system_prompt = (
            "You are a data generation assistant that converts natural language "
            "descriptions of synthetic data generation scenarios into structured YAML. "
            "Only output the YAML under a 'scenario:' root key."
        )
        
        user_prompt = f"Generate a scenario YAML for this use case:\n\n{nl_prompt}\n\nYAML:"
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=self.temperature,
            max_tokens=400,
        )
    
        yaml_text = response.choices[0].message.content.strip()
        print(f"OpenAI response:\n{yaml_text}")

        # Strip markdown code block markers if present
        if yaml_text.startswith("```"):
            # Remove `````` from start and ```
            yaml_text = '\n'.join(line for line in yaml_text.splitlines() if not line.strip().startswith('```'))

        # Parse YAML response safely
        try:
            scenario_dict = yaml.safe_load(yaml_text)
            if "scenario" in scenario_dict:
                return scenario_dict["scenario"]
            else:
                raise ValueError("YAML missing 'scenario' key")
        except Exception as e:
            raise ValueError(f"Error parsing YAML from OpenAI response: {e}")
