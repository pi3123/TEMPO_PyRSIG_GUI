"""
Chart Intent Generator using Google Gemini API.

This module generates ChartIntent specifications from natural language queries.
Instead of generating code, it outputs structured JSON that can be executed
safely and efficiently.
"""

import logging
import time
from typing import Any

from .config import ConfigManager
from .chart_intent import ChartIntent, ChartIntentError, parse_intent_from_response


class ChartGenerationError(Exception):
    """Raised when chart intent generation fails."""
    pass


class ChartGenerator:
    """
    Generates ChartIntent from natural language using Gemini API.

    Usage:
        generator = ChartGenerator()
        intent = generator.generate_intent(
            query="Plot NO2 trends by hour",
            schema={"columns": [...], "dtypes": {...}}
        )
    """

    def __init__(self):
        """Initialize the chart generator with Gemini API."""
        self.config = ConfigManager()
        self._model = None
        self._client = None
        self._cache: dict[int, ChartIntent] = {}  # Cache for identical queries
        self.logger = logging.getLogger(__name__)

    # Class-level cache for model list (persists across instances)
    _model_cache: dict = {}
    _model_cache_time: float = 0
    _MODEL_CACHE_TTL = 24 * 60 * 60  # 24 hours in seconds

    @staticmethod
    def list_available_models(api_key: str, use_cache: bool = True) -> list[dict]:
        """
        List available Gemini models with caching.
        
        Args:
            api_key: Gemini API key
            use_cache: Whether to use cached results (default True)
            
        Returns:
            List of model info dicts with 'name' and 'display_name'
        """
        import re
        
        # Check cache first
        cache_key = hash(api_key)
        if use_cache and cache_key in ChartGenerator._model_cache:
            cached_time = ChartGenerator._model_cache_time
            if time.time() - cached_time < ChartGenerator._MODEL_CACHE_TTL:
                return ChartGenerator._model_cache[cache_key]
        
        try:
            from google import genai
            client = genai.Client(api_key=api_key)
            
            special_latest_models = []
            models_by_version = {}
            
            for model in client.models.list():
                # Check generation support
                supported = getattr(model, 'supported_actions', None) or getattr(model, 'supported_generation_methods', [])
                
                if 'generateContent' in supported:
                    name = model.name.replace('models/', '')
                    
                    if 'gemini' in name.lower() and 'image' not in name.lower() and 'vision' not in name.lower():
                        
                        model_info = {
                            'name': name,
                            'display_name': getattr(model, 'display_name', name),
                        }

                        if name.endswith('-latest'):
                            special_latest_models.append(model_info)
                            continue

                        match = re.search(r'gemini-(\d+(?:\.\d+)?)', name)
                        
                        if match:
                            version_str = match.group(1)
                            try:
                                version_val = float(version_str)
                            except ValueError:
                                continue
                                
                            if version_val not in models_by_version:
                                models_by_version[version_val] = []
                            
                            models_by_version[version_val].append(model_info)

            final_models = []
            if models_by_version:
                max_version = max(models_by_version.keys())
                final_models.extend(models_by_version[max_version])
            
            final_models.extend(special_latest_models)

            seen = set()
            unique_models = []
            for m in final_models:
                if m['name'] not in seen:
                    unique_models.append(m)
                    seen.add(m['name'])
            
            result = sorted(unique_models, key=lambda x: x['name'], reverse=True)
            
            ChartGenerator._model_cache[cache_key] = result
            ChartGenerator._model_cache_time = time.time()
            
            return result
            
        except Exception as e:
            logging.error(f"[CHART_GEN] Error listing models: {e}")
            if cache_key in ChartGenerator._model_cache:
                return ChartGenerator._model_cache[cache_key]
            return []

    def _setup_gemini(self) -> None:
        """Configure Gemini API with user's key (lazy initialization)."""
        if self._model is not None:
            return
            
        try:
            from google import genai
        except ImportError:
            raise ChartGenerationError(
                "google-genai package not installed. "
                "Run: pip install google-genai"
            )
        
        api_key = self.config.get("gemini_api_key")

        if not api_key:
            raise ChartGenerationError(
                "Gemini API key not configured. "
                "Please add your API key in Settings."
            )

        model_name = self.config.get("gemini_model", "gemini-2.0-flash-lite")
        
        self._client = genai.Client(api_key=api_key)
        self._model = model_name

    def generate_intent(
        self,
        query: str,
        schema: dict[str, Any]
    ) -> ChartIntent:
        """
        Generate a ChartIntent from a natural language query.

        Args:
            query: Natural language description (e.g., "Plot NO2 by hour")
            schema: Dataset schema with columns, dtypes, sample_values

        Returns:
            ChartIntent specification for the chart

        Raises:
            ChartGenerationError: If API call fails or response is invalid
        """
        self.logger.info(f"[CHART_GEN] Generating intent for: {query}")
        
        # Lazy initialize Gemini
        self._setup_gemini()
        
        # Check cache
        cache_key = hash((query, tuple(sorted(schema.get("columns", [])))))
        if cache_key in self._cache:
            self.logger.info("[CHART_GEN] Using cached intent")
            return self._cache[cache_key]

        # Build the prompt
        prompt = self._build_intent_prompt(query, schema)

        try:
            self.logger.info("[CHART_GEN] Calling Gemini API...")
            
            response = self._client.models.generate_content(
                model=self._model,
                contents=prompt
            )

            if not response.text:
                raise ChartGenerationError("Gemini returned empty response")

            self.logger.info(f"[CHART_GEN] Response: {response.text[:200]}...")
            
            # Parse response into ChartIntent
            intent = parse_intent_from_response(response.text, schema)
            
            # Cache the result
            self._cache[cache_key] = intent
            
            self.logger.info(f"[CHART_GEN] Generated intent: {intent}")
            return intent

        except ChartIntentError as e:
            raise ChartGenerationError(str(e))
        except ChartGenerationError:
            raise
        except Exception as e:
            raise ChartGenerationError(f"Failed to generate intent: {str(e)}")

    def _build_intent_prompt(self, query: str, schema: dict[str, Any]) -> str:
        """
        Build prompt for JSON intent generation.
        
        The prompt instructs Gemini to output structured JSON, not code.
        """
        columns = schema.get("columns", [])
        columns_str = ", ".join(columns)
        
        # Include sample values for context
        samples_str = ""
        if "sample_values" in schema:
            samples = schema["sample_values"]
            samples_str = "\nSample values:\n" + "\n".join([
                f"  {col}: {vals}" for col, vals in samples.items()
            ])

        prompt = f"""You are a data visualization assistant. Output a JSON chart specification.

DATASET COLUMNS: {columns_str}
{samples_str}

USER QUERY: {query}

OUTPUT FORMAT - Return ONLY a JSON object (no markdown, no explanation):
{{
  "chart_type": "line" or "bar" or "scatter" or "histogram",
  "x": "hour" or "date" or "month" or "day_of_week" or column_name,
  "y": ["column1"] or ["column1", "column2"] or ["column1 / column2"],
  "aggregation": "mean" or "sum" or "count" or "min" or "max",
  "filters": {{}},
  "title": "optional custom title"
}}

RULES:
1. y is ALWAYS a list (even for single column)
2. For comparing multiple variables: y = ["NO2_TropVCD", "HCHO_TropVCD"]
3. For ratios/expressions: y = ["NO2_TropVCD / HCHO_TropVCD"]
4. For "by hour": x = "hour"
5. For trends: x = "date"
6. Use exact column names from DATASET COLUMNS

EXAMPLES:
Query: "Show NO2 by hour" → {{"chart_type": "line", "x": "hour", "y": ["NO2_TropVCD"]}}
Query: "Compare NO2 and HCHO" → {{"chart_type": "line", "x": "hour", "y": ["NO2_TropVCD", "HCHO_TropVCD"]}}
Query: "NO2/HCHO ratio by hour" → {{"chart_type": "line", "x": "hour", "y": ["NO2_TropVCD / HCHO_TropVCD"]}}
Query: "Daily trends" → {{"chart_type": "line", "x": "date", "y": ["NO2_TropVCD"]}}

Now output JSON for: {query}"""

        return prompt
